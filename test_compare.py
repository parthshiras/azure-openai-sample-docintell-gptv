#!/usr/bin/env python3
"""
Test script for document comparison functionality
"""
import os
import sys
import requests
import json

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
                print_comparison_result(result)
                return True
            else:
                print(f"❌ API request failed: {response.status_code}")
                print(f"Error: {response.text}")
                return False
                
    except Exception as e:
        print(f"❌ Error during comparison: {e}")
        return False

def print_comparison_result(result):
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

def test_direct_comparison(doc1_path, doc2_path):
    """Test direct document comparison (without API)"""
    print("🔄 Testing direct comparison...")
    
    try:
        from document_compare import compare_documents, print_comparison_summary
        
        results = compare_documents(doc1_path, doc2_path)
        print_comparison_summary(results)
        
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