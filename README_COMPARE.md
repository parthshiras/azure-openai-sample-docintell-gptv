# Document Comparison System

This system compares two document scans to determine if they contain the same content, even when the scans have different orientations, lighting, or offsets.

## Features

🔍 **Content Analysis**: Uses Azure Document Intelligence for OCR and text extraction  
🧠 **AI Comparison**: Leverages GPT-4 Vision for visual and semantic analysis  
📊 **Multiple Metrics**: Provides text similarity, visual similarity, and confidence scores  
🌐 **REST API**: Flask-based API for easy integration  
📄 **Multi-format**: Supports PDF, JPG, PNG, TIFF, and other image formats  

## How It Works

1. **Text Extraction**: Azure Document Intelligence extracts text, tables, and key-value pairs from both documents
2. **Text Comparison**: Calculates similarity score using sequence matching algorithms
3. **Visual Analysis**: GPT-4 Vision compares the visual layout and structure of documents
4. **Smart Comparison**: Focuses on content rather than pixel-perfect matching
5. **Comprehensive Results**: Returns detailed analysis with confidence scores and identified differences

## Quick Start

### 1. Start the API Server
```bash
python app_compare.py
```

### 2. Compare Two Documents
```bash
# Using the test script
python test_compare.py document1.pdf document2.pdf

# Using curl
curl -X POST \
  -F "document1=@doc1.pdf" \
  -F "document2=@doc2.pdf" \
  http://localhost:5000/compare
```

### 3. Direct Python Usage
```python
from document_compare import compare_documents

results = compare_documents("doc1.pdf", "doc2.pdf")
print(f"Same document: {results['gpt_analysis']['same_document']}")
```

## API Endpoints

### `POST /compare`
Compare two documents

**Parameters:**
- `document1`: File upload (required)
- `document2`: File upload (required)

**Response:**
```json
{
  "text_similarity": 0.85,
  "gpt_analysis": {
    "same_document": true,
    "confidence_score": 0.92,
    "visual_similarity": "high",
    "key_differences": [],
    "analysis_notes": "Documents appear to be the same with minor scan quality differences"
  },
  "document1_stats": {
    "page_count": 1,
    "text_length": 1250,
    "table_count": 2
  },
  "document2_stats": {
    "page_count": 1,
    "text_length": 1248,
    "table_count": 2
  }
}
```

### `GET /health`
Check API health status

### `GET /`
Get API information and usage examples

## Supported File Formats

- **Images**: PNG, JPG, JPEG, GIF, BMP, TIFF
- **Documents**: PDF
- **Size Limit**: 16MB per file

## Use Cases

✅ **Document Verification**: Verify if a scanned document matches an original  
✅ **Duplicate Detection**: Find duplicate documents in different formats  
✅ **Quality Assurance**: Compare documents before/after processing  
✅ **Compliance**: Ensure document integrity across systems  
✅ **Archive Management**: Identify duplicate documents in archives  

## Configuration

The system uses the same Azure resources as the original application:

- **Azure OpenAI**: For GPT-4 Vision analysis
- **Azure Document Intelligence**: For OCR and content extraction

Environment variables are loaded from `.env`:
```bash
AZURE_OPENAI_API_DEPLOYMENT=gpt-4o
AZURE_OPENAI_API_KEY=your-key
AZURE_OPENAI_ENDPOINT=your-endpoint
DOC_INTEL_ENDPOINT=your-doc-intel-endpoint
DOC_INTEL_KEY=your-doc-intel-key
```

## Comparison Logic

The system uses multiple approaches to ensure accurate comparison:

1. **Text Similarity**: Normalized text comparison using sequence matching
2. **Structure Analysis**: Compares tables, key-value pairs, and document layout
3. **Visual Analysis**: GPT-4 Vision analyzes visual elements and layout
4. **Content Focus**: Prioritizes content over scan quality differences
5. **Confidence Scoring**: Provides reliability metrics for the comparison

## Example Results

### Same Document (Different Scans)
```
Same Document: ✅ YES
Confidence: 0.95
Text Similarity: 0.88
Visual Similarity: high
Analysis Notes: Minor rotation and lighting differences, but content is identical
```

### Different Documents
```
Same Document: ❌ NO  
Confidence: 0.89
Text Similarity: 0.23
Visual Similarity: low
Key Differences: ["Different headers", "Different table content", "Different signatures"]
```

## Troubleshooting

### Common Issues

1. **Low confidence scores**: May indicate poor scan quality or significant differences
2. **High text similarity, low visual similarity**: Could indicate reformatted versions of the same content
3. **API connection errors**: Ensure the Flask app is running on port 5000

### Performance Tips

- **Image Quality**: Higher quality scans provide better results
- **File Size**: Keep files under 10MB for faster processing
- **Format**: PDF files often provide better text extraction than images

## Testing

Run the comprehensive test suite:
```bash
python test_compare.py doc1.pdf doc2.pdf
```

This tests both direct comparison and API functionality.