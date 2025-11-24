from typing import Dict, Any, List, Union
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.documents import Document

class LLMChainConfig:
    # Model settings
    LLM_MODEL = "gpt-4.1-mini"
    TEMPERATURE = 0.1
    
    # Prompt settings
    SYSTEM_PROMPT = """You are ChatBGP, a specialized BGP analysis assistant that combines RFC documentation with live routing data and security analysis.

You receive structured context data that may include:

1. **static_docs**: RFC documentation for BGP concepts and standards
   - Always cite specific RFC numbers when using this information
   - Use exact terminology from BGP standards
   - Reference sections and requirements with RFC citations

2. **live_bgp**: Current routing state from radix trees
   - Interpret prefix announcements, AS paths, and origin information
   - Explain longest prefix matches vs exact prefix lookups
   - Describe practical routing implications

3. **historical**: BGP update history from database
   - Analyze routing changes and stability patterns
   - Identify origin AS changes or path modifications
   - Highlight unusual routing behavior or instability

4. **validation**: RPKI and IRR validation results
   - Explain validation status (valid, invalid, not-found)
   - Describe security implications of validation results
   - Note discrepancies between expected and actual announcements

5. **analysis**: Automated heuristic analysis results
   - **CRITICAL**: If analysis indicates potential hijacks or anomalies, highlight these prominently
   - Explain detected discrepancies and their security implications
   - Provide context for automated findings

**Response Guidelines:**
- Always reference RFC numbers when citing documentation
- Highlight security concerns and anomalies prominently
- Use precise BGP terminology
- Explain technical concepts clearly
- If data is missing or limited, state limitations clearly
- When multiple data sources are available, synthesize them coherently"""

    HUMAN_PROMPT = """## Context Information
{context}

## Entity Information
{entities}

## User Question
{question}

Please provide a detailed analysis using the most relevant information from the context provided. If you notice any routing anomalies or security concerns, highlight them prominently."""

class BGPChain:
    """Handles LLM chain configuration and execution."""
    
    def __init__(self, config: LLMChainConfig = LLMChainConfig()):
        self.config = config
        self._init_chain()

    def _init_chain(self):
        """Initialize the LLM and chain."""
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=self.config.LLM_MODEL,
            temperature=self.config.TEMPERATURE
        )

        # Create prompt template
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", self.config.SYSTEM_PROMPT),
            ("human", self.config.HUMAN_PROMPT)
        ])

        # Create document chain
        self.chain = create_stuff_documents_chain(
            llm=self.llm,
            prompt=self.prompt
        )

    def _prepare_context(self, context: Union[str, List[Document], List[str]]) -> List[Document]:
        """Convert context to proper Document format if needed."""
        if isinstance(context, str):
            return [Document(page_content=context)]
        elif isinstance(context, list):
            if all(isinstance(doc, Document) for doc in context):
                return context
            else:
                return [Document(page_content=text) for text in context if isinstance(text, str)]
        return []

    def _format_entities_for_prompt(self, entities: Dict[str, Any], query_types: List[str]) -> str:
        """Format extracted entities and query types for the prompt"""
        if not entities and not query_types:
            return ""
            
        parts = []
        
        # Add query types
        if query_types:
            parts.append(f"Query Types: {', '.join(query_types)}")
        
        # Add IP addresses
        if entities.get("ip_addresses"):
            parts.append(f"IP Addresses: {', '.join(entities['ip_addresses'])}")
            
        # Add prefixes 
        if entities.get("prefixes"):
            parts.append(f"Prefixes: {', '.join(entities['prefixes'])}")
            
        # Add AS numbers
        if entities.get("asns"):
            parts.append(f"AS Numbers: {', '.join(['AS' + asn for asn in entities['asns']])}")
            
        # Add time references
        if entities.get("time_references"):
            parts.append(f"Time References: {', '.join(entities['time_references'])}")
            
        return "\n".join(parts)

    def generate_response(self, context: Union[str, List[Document], List[str]], query: str, entities: Dict[str, Any] = None, query_types: List[str] = None) -> str:
        """Generate a response using the LLM chain."""
        # Prepare context in the correct format
        docs = self._prepare_context(context)
        
        # Format entities for prompt if provided
        formatted_entities = self._format_entities_for_prompt(entities or {}, query_types or [])
        
        # Generate response
        response = self.chain.invoke({
            "context": docs,
            "entities": formatted_entities,
            "question": query
        })
        
        return response["answer"] if isinstance(response, dict) else response