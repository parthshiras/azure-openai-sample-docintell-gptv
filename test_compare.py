#!/usr/bin/env python3
"""
Test script for document comparison functionality
"""
import os
import sys
import requests
import json
import difflib
from textwrap import wrap

def generate_text_diff(doc1_path, doc2_path, show_diff_threshold=0.8):
    """Generate a text diff between two documents if they're different enough"""
    try:
        from document_compare import extract_document_content
        
        # Extract text from both documents
        doc1_content = extract_document_content(doc1_path)
        doc2_content = extract_document_content(doc2_path)
        
        # Split text into lines for comparison
        doc1_lines = doc1_content.text.splitlines()
        doc2_lines = doc2_content.text.splitlines()
        
        # Calculate similarity
        from difflib import SequenceMatcher
        similarity = SequenceMatcher(None, doc1_content.text, doc2_content.text).ratio()
        
        # Only show diff if documents are different enough
        if similarity < show_diff_threshold:
            print(f"\n📄 TEXT DIFF ANALYSIS")
            print("=" * 60)
            print(f"Similarity: {similarity:.2f} (threshold: {show_diff_threshold})")
            
            # Generate unified diff
            diff = list(difflib.unified_diff(
                doc1_lines,
                doc2_lines,
                fromfile=f"Document 1 ({os.path.basename(doc1_path)})",
                tofile=f"Document 2 ({os.path.basename(doc2_path)})",
                lineterm='',
                n=3  # Context lines
            ))
            
            if diff:
                print(f"\n🔄 Text Differences:")
                print("-" * 40)
                
                # Show first 50 lines of diff to avoid overwhelming output
                for i, line in enumerate(diff[:50]):
                    if line.startswith('+++') or line.startswith('---'):
                        print(f"\033[96m{line}\033[0m")  # Cyan for file headers
                    elif line.startswith('@@'):
                        print(f"\033[94m{line}\033[0m")  # Blue for context
                    elif line.startswith('+'):
                        print(f"\033[92m{line}\033[0m")  # Green for additions
                    elif line.startswith('-'):
                        print(f"\033[91m{line}\033[0m")  # Red for deletions
                    else:
                        print(line)
                
                if len(diff) > 50:
                    print(f"\n... ({len(diff) - 50} more diff lines truncated)")
                
                print("-" * 40)
            else:
                print("No significant line-by-line differences found")
                
            # Show character-level differences for short texts
            if len(doc1_content.text) < 1000 and len(doc2_content.text) < 1000:
                print(f"\n🔤 Character-level differences:")
                char_diff = list(difflib.ndiff([doc1_content.text], [doc2_content.text]))
                for line in char_diff:
                    if line.startswith('+ '):
                        print(f"Document 2: {line[2:]}")
                    elif line.startswith('- '):
                        print(f"Document 1: {line[2:]}")
        
        return similarity
        
    except Exception as e:
        print(f"❌ Error generating diff: {e}")
        return None

def test_api_health():
    """Test if the API is running"""
    try:
        response = requests.get('http://localhost:5000/health')
        if response.status_code == 200:
            print("✅ API is healthy")
            return True
        else:
            print(f"❌ API health check failed: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API. Make sure it's running on port 5000")
        return False

def test_document_comparison(doc1_path, doc2_path):
    """Test document comparison via API"""
    if not os.path.exists(doc1_path):
        print(f"❌ Document 1 not found: {doc1_path}")
        return False
    
    if not os.path.exists(doc2_path):
        print(f"❌ Document 2 not found: {doc2_path}")
        return False
    
    print(f"📄 Comparing documents:")
    print(f"  Document 1: {doc1_path}")
    print(f"  Document 2: {doc2_path}")
    
    try:
        with open(doc1_path, 'rb') as f1, open(doc2_path, 'rb') as f2:
            files = {
                'document1': f1,
                'document2': f2
            }
            
            response = requests.post('http://localhost:5000/compare', files=files)
            
            if response.status_code == 200:
                result = response.json()
                print_comparison_result(result, doc1_path, doc2_path)
                return True
            else:
                print(f"❌ API request failed: {response.status_code}")
                print(f"Error: {response.text}")
                return False
                
    except Exception as e:
        print(f"❌ Error during comparison: {e}")
        return False

def print_comparison_result(result, doc1_path=None, doc2_path=None):
    """Print formatted comparison results"""
    print("\n" + "="*60)
    print("📋 DOCUMENT COMPARISON RESULTS")
    print("="*60)
    
    gpt_analysis = result.get('gpt_analysis', {})
    
    same_doc = gpt_analysis.get('same_document', False)
    confidence = gpt_analysis.get('confidence_score', 0)
    text_sim = result.get('text_similarity', 0)
    visual_sim = gpt_analysis.get('visual_similarity', 'unknown')
    
    print(f"Same Document: {'✅ YES' if same_doc else '❌ NO'}")
    print(f"Confidence: {confidence:.2f}")
    print(f"Text Similarity: {text_sim:.2f}")
    print(f"Visual Similarity: {visual_sim}")
    
    differences = gpt_analysis.get('key_differences', [])
    if differences:
        print(f"\n🔍 Key Differences:")
        for diff in differences:
            print(f"  • {diff}")
    
    notes = gpt_analysis.get('analysis_notes', '')
    if notes:
        print(f"\n📝 Analysis Notes:")
        print(f"  {notes}")
    
    # Document stats
    doc1_stats = result.get('document1_stats', {})
    doc2_stats = result.get('document2_stats', {})
    
    print(f"\n📊 Document Statistics:")
    print(f"  Doc 1: {doc1_stats.get('text_length', 0)} chars, {doc1_stats.get('page_count', 0)} pages")
    print(f"  Doc 2: {doc2_stats.get('text_length', 0)} chars, {doc2_stats.get('page_count', 0)} pages")
    
    # Show diff if documents are different and we have file paths
    if not same_doc and doc1_path and doc2_path:
        print(f"\n💡 Generating detailed text diff...")
        generate_text_diff(doc1_path, doc2_path, show_diff_threshold=0.9)

def test_direct_comparison(doc1_path, doc2_path):
    """Test direct document comparison (without API)"""
    print("🔄 Testing direct comparison...")
    
    try:
        from document_compare import compare_documents
        
        results = compare_documents(doc1_path, doc2_path)
        
        # Use our enhanced print function that shows diffs
        print_comparison_result(results, doc1_path, doc2_path)
        
        return True
    except Exception as e:
        print(f"❌ Direct comparison failed: {e}")
        return False

def main():
    print("🧪 Document Comparison Test Suite")
    print("=" * 40)
    
    if len(sys.argv) == 3:
        doc1_path = sys.argv[1]
        doc2_path = sys.argv[2]
        
        print(f"📄 Testing with provided documents:")
        print(f"  Document 1: {doc1_path}")
        print(f"  Document 2: {doc2_path}")
        print()
        
        # Test 1: Direct comparison
        print("🔍 Test 1: Direct Comparison")
        test_direct_comparison(doc1_path, doc2_path)
        
        print("\n" + "-" * 60)
        
        # Test 2: API comparison
        print("🌐 Test 2: API Comparison")
        if test_api_health():
            test_document_comparison(doc1_path, doc2_path)
        else:
            print("💡 To start the API: python app_compare.py")
    
    else:
        print("Usage: python test_compare.py <document1_path> <document2_path>")
        print("\nExample:")
        print("  python test_compare.py doc1.pdf doc2.pdf")
        print("  python test_compare.py scan1.jpg scan2.jpg")
        print("\nMake sure to start the API first:")
        print("  python app_compare.py")

if __name__ == "__main__":
    main()