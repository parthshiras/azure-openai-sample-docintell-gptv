import base64
import os
import logging
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
from difflib import SequenceMatcher

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_openai import AzureChatOpenAI
from pydantic import BaseModel, Field

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult
from azure.core.credentials import AzureKeyCredential

load_dotenv('.env')

# Environment variables
AZURE_OPENAI_API_DEPLOYMENT = os.getenv("AZURE_OPENAI_API_DEPLOYMENT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
DOC_INTEL_ENDPOINT = os.getenv("DOC_INTEL_ENDPOINT")
DOC_INTEL_KEY = os.getenv("DOC_INTEL_KEY")

# Initialize Azure OpenAI
llm = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    azure_deployment=AZURE_OPENAI_API_DEPLOYMENT,
    openai_api_version=OPENAI_API_VERSION,
    openai_api_key=AZURE_OPENAI_API_KEY,
    temperature=0,
    max_tokens=2000,
    verbose=True
)

# Initialize Document Intelligence
kwargs = {"api_version": "2023-10-31-preview"}
doc_client = DocumentIntelligenceClient(
    endpoint=DOC_INTEL_ENDPOINT,
    credential=AzureKeyCredential(DOC_INTEL_KEY),
    **kwargs
)


@dataclass
class DocumentContent:
    """Holds extracted content from a document"""
    text: str
    structured_content: str
    page_count: int
    tables: list
    key_value_pairs: dict


class DocumentComparison(BaseModel):
    """Structured output for document comparison analysis"""
    same_document: bool = Field(description="True if the documents appear to be the same document")
    confidence_score: float = Field(description="Confidence score from 0.0 to 1.0")
    text_similarity: float = Field(description="Text similarity score from 0.0 to 1.0")
    visual_similarity: str = Field(description="Visual similarity assessment (high/medium/low)")
    key_differences: list[str] = Field(description="List of key differences found")
    analysis_notes: str = Field(description="Additional notes about the comparison")


def encode_image(image_path: str) -> str:
    """Encode image as base64"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')


def extract_document_content(file_path: str) -> DocumentContent:
    """Extract content from document using Azure Document Intelligence"""
    with open(file_path, "rb") as f:
        poller = doc_client.begin_analyze_document(
            "prebuilt-layout",
            f,
            locale="en-US",
            content_type="application/octet-stream",
            output_content_format="markdown"
        )
    result = poller.result()
    
    # Extract tables
    tables = []
    if result.tables:
        for table in result.tables:
            table_data = []
            for cell in table.cells:
                table_data.append({
                    'content': cell.content,
                    'row': cell.row_index,
                    'column': cell.column_index
                })
            tables.append(table_data)
    
    # Extract key-value pairs
    key_value_pairs = {}
    if result.key_value_pairs:
        for kv_pair in result.key_value_pairs:
            if kv_pair.key and kv_pair.value:
                key_value_pairs[kv_pair.key.content] = kv_pair.value.content
    
    return DocumentContent(
        text=result.content or "",
        structured_content=result.content or "",
        page_count=len(result.pages) if result.pages else 1,
        tables=tables,
        key_value_pairs=key_value_pairs
    )


def calculate_text_similarity(text1: str, text2: str) -> float:
    """Calculate similarity between two text strings"""
    # Normalize texts (remove extra whitespace, convert to lowercase)
    norm_text1 = ' '.join(text1.lower().split())
    norm_text2 = ' '.join(text2.lower().split())
    
    # Use sequence matcher for similarity
    similarity = SequenceMatcher(None, norm_text1, norm_text2).ratio()
    return similarity


def compare_documents_with_gpt(image1_path: str, image2_path: str, 
                             doc1_content: DocumentContent, 
                             doc2_content: DocumentContent) -> DocumentComparison:
    """Use GPT-4 Vision to compare two document images"""
    
    # Calculate text similarity
    text_similarity = calculate_text_similarity(doc1_content.text, doc2_content.text)
    
    # Check if files are PDFs (GPT-4 Vision can't process PDFs directly)
    is_pdf1 = image1_path.lower().endswith('.pdf')
    is_pdf2 = image2_path.lower().endswith('.pdf')
    
    if is_pdf1 or is_pdf2:
        # For PDFs, do text-based analysis only
        prompt = f"""
        You are comparing two documents based on their extracted text content to determine if they represent the same document.
        
        Document 1 extracted text:
        {doc1_content.text[:2000]}...
        
        Document 2 extracted text:
        {doc2_content.text[:2000]}...
        
        Text similarity score (calculated): {text_similarity:.2f}
        
        Document 1 stats: {doc1_content.page_count} pages, {len(doc1_content.tables)} tables
        Document 2 stats: {doc2_content.page_count} pages, {len(doc2_content.tables)} tables
        
        Based on the text content and structure, determine:
        1. Are these the same document?
        2. What's your confidence level?
        3. What are the key similarities/differences?
        
        Note: Visual analysis not available for PDF files - analysis based on text content only.
        """
        
        parser = PydanticOutputParser(pydantic_object=DocumentComparison)
        
        message = HumanMessage(
            content=[
                {"type": "text", "text": prompt},
                {"type": "text", "text": parser.get_format_instructions()},
            ]
        )
        
        response = llm.invoke([message])
        return parser.parse(response.content)
    
    else:
        # For image files, use visual analysis
        image1_b64 = encode_image(image1_path)
        image2_b64 = encode_image(image2_path)
        
        prompt = f"""
        You are tasked with comparing two document scans to determine if they represent the same document.
        The documents may be scanned at different angles, have different lighting, or be slightly offset, 
        but you need to focus on the CONTENT rather than pixel-perfect matching.
        
        Document 1 extracted text:
        {doc1_content.text[:1500]}...
        
        Document 2 extracted text:
        {doc2_content.text[:1500]}...
        
        Text similarity score (calculated): {text_similarity:.2f}
        
        Please analyze both images and the extracted text to determine:
        1. Are these the same document? (consider content, layout, structure)
        2. What's your confidence level?
        3. What are the key visual similarities/differences?
        4. Any notable discrepancies?
        
        Focus on content and structure rather than image quality differences.
        """
        
        parser = PydanticOutputParser(pydantic_object=DocumentComparison)
        
        message = HumanMessage(
            content=[
                {"type": "text", "text": prompt},
                {"type": "text", "text": parser.get_format_instructions()},
                {"type": "text", "text": "Document 1:"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image1_b64}"}},
                {"type": "text", "text": "Document 2:"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image2_b64}"}},
            ]
        )
        
        response = llm.invoke([message])
        return parser.parse(response.content)


def compare_documents(doc1_path: str, doc2_path: str) -> Dict:
    """
    Main function to compare two documents
    Returns comprehensive comparison results
    """
    print(f"🔍 Comparing documents:")
    print(f"  Document 1: {doc1_path}")
    print(f"  Document 2: {doc2_path}")
    
    # Extract content from both documents
    print("📄 Extracting content from Document 1...")
    doc1_content = extract_document_content(doc1_path)
    
    print("📄 Extracting content from Document 2...")
    doc2_content = extract_document_content(doc2_path)
    
    # Calculate basic metrics
    text_similarity = calculate_text_similarity(doc1_content.text, doc2_content.text)
    
    print(f"📊 Text similarity score: {text_similarity:.2f}")
    
    # Use GPT-4 Vision for visual and content comparison
    print("🧠 Analyzing with GPT-4 Vision...")
    gpt_comparison = compare_documents_with_gpt(doc1_path, doc2_path, doc1_content, doc2_content)
    
    # Compile comprehensive results
    results = {
        "document1_path": doc1_path,
        "document2_path": doc2_path,
        "text_similarity": text_similarity,
        "gpt_analysis": gpt_comparison.model_dump(),
        "document1_stats": {
            "page_count": doc1_content.page_count,
            "text_length": len(doc1_content.text),
            "table_count": len(doc1_content.tables),
            "key_value_pairs": len(doc1_content.key_value_pairs)
        },
        "document2_stats": {
            "page_count": doc2_content.page_count,
            "text_length": len(doc2_content.text),
            "table_count": len(doc2_content.tables),
            "key_value_pairs": len(doc2_content.key_value_pairs)
        }
    }
    
    return results


def print_comparison_summary(results: Dict):
    """Print a formatted summary of the comparison results"""
    gpt = results["gpt_analysis"]
    
    print("\n" + "="*60)
    print("📋 DOCUMENT COMPARISON SUMMARY")
    print("="*60)
    print(f"Same Document: {'✅ YES' if gpt['same_document'] else '❌ NO'}")
    print(f"Confidence Score: {gpt['confidence_score']:.2f}")
    print(f"Text Similarity: {results['text_similarity']:.2f}")
    print(f"Visual Similarity: {gpt['visual_similarity']}")
    
    if gpt['key_differences']:
        print(f"\n🔍 Key Differences:")
        for diff in gpt['key_differences']:
            print(f"  • {diff}")
    
    if gpt['analysis_notes']:
        print(f"\n📝 Analysis Notes:")
        print(f"  {gpt['analysis_notes']}")
    
    print(f"\n📊 Document Statistics:")
    print(f"  Doc 1: {results['document1_stats']['text_length']} chars, {results['document1_stats']['page_count']} pages")
    print(f"  Doc 2: {results['document2_stats']['text_length']} chars, {results['document2_stats']['page_count']} pages")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python document_compare.py <document1_path> <document2_path>")
        sys.exit(1)
    
    doc1_path = sys.argv[1]
    doc2_path = sys.argv[2]
    
    try:
        results = compare_documents(doc1_path, doc2_path)
        print_comparison_summary(results)
        
        # Optionally save results to JSON
        import json
        output_file = "comparison_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n💾 Detailed results saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error comparing documents: {e}")
        import traceback
        traceback.print_exc()