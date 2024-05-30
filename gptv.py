import base64
import inspect
import os
import logging
import io

from dotenv import load_dotenv
from langchain.chains.transform import TransformChain
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import chain
from langchain_openai import AzureChatOpenAI
from pydantic import BaseModel, Field

load_dotenv('.env')
# Needs following environment variables:
AZURE_OPENAI_API_DEPLOYMENT = os.getenv("AZURE_OPENAI_API_DEPLOYMENT")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
DOC_INTEL_ENDPOINT = os.getenv("DOC_INTEL_ENDPOINT")
DOC_INTEL_KEY = os.getenv("DOC_INTEL_KEY")

llm = AzureChatOpenAI(
    azure_endpoint= AZURE_OPENAI_ENDPOINT,
    azure_deployment=AZURE_OPENAI_API_DEPLOYMENT,
    openai_api_version = OPENAI_API_VERSION,
    openai_api_key= AZURE_OPENAI_API_KEY,
    temperature=0,
    max_tokens=1000,
    verbose=True)


def load_image(inputs: dict) -> dict:
    """Load image from file and encode it as base64."""
    image_path = inputs["image_path"]

    def encode_image(image_path):
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')

    image_base64 = encode_image(image_path)
    return {"image": image_base64}


# This piece-of-chain is used later, that loads an image from a file and encodes to base64 with the previous function.
load_image_chain = TransformChain(
    input_variables=["image_path"],
    output_variables=["image"],
    transform=load_image
)


# Pydantic schema for the image information. Descriptions are important (used by the model).
class ImageInformation(BaseModel):
    """Information about a product image."""
    brand: str = Field(default="n/a", description="Name of the brand, or n/a")
    product_name: str = Field(default="n/a", description="Name of the product, or n/a")
    price: str = Field(default="n/a", description="price of the product, or n/a")
    price_per_unit: str = Field(default="n/a", description="price of the product per unit, or n/a")
    expiration_date: str = Field(default="n/a", description="product's date of expiration/best-before, or n/a")
    article_number: str = Field(default="n/a", description="Article Number of the product, or n/a")
    bar_code_available: bool = Field(default=False, description="is there bar-code in the image")
    bar_code_numbers: str = Field(default=False, description="Numbers of the bar-code, or n/a")


parser = PydanticOutputParser(pydantic_object=ImageInformation)


# piece-of-chain that invokes the model with the image and prompt.
@chain
def gpt_vision(inputs: dict) -> str | list[str] | dict:
    """Invoke model with image and prompt."""
    model = llm
    msg = model.invoke(
        [HumanMessage(
            content=[
                {"type": "text", "text": inputs["prompt"]},
                {"type": "text", "text": parser.get_format_instructions()},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{inputs['image']}"}},
            ])]
    )
    return msg.content


def analyze_with_gpt(image_path: str, doc_int_results: str, doc_int_barcode: str) -> ImageInformation:
    vision_prompt = inspect.cleandoc("""
    Given the image, provide the following information:
    - Brand
    - Product Name
    - Price
    - Price per unit
    - Expiration Date (or best-before)
    - Article Number (if available, it will be multiple numbers seperated by a period)
    - Is there a bar-code available?
    - bar-code numbers (if available)
    
    We also found following text with other OCR tool, you can use it to enhance the results:
    {doc_int_results}
    
    The other tool also found following bar-code: {doc_int_barcode}
    Use it if you can.
    
   """)
    template = PromptTemplate(
        input_variables=["doc_int_results", "doc_int_barcode"], template="Question: {question}\n{answer}"
    )
    vision_chain = load_image_chain | gpt_vision | parser
    return vision_chain.invoke({'image_path': image_path,
                                'prompt': vision_prompt.format(doc_int_results=doc_int_results,doc_int_barcode=doc_int_barcode)})


#############################################
# Setup Document intelligence for barcodes  #
#############################################

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult, DocumentAnalysisFeature
from azure.core.credentials import AzureKeyCredential

kwargs = {"api_version": "2023-10-31-preview"}
client = document_analysis_client = DocumentIntelligenceClient(endpoint=DOC_INTEL_ENDPOINT,
                                                               credential=AzureKeyCredential(DOC_INTEL_KEY),
                                                               **kwargs)


def get_doc_int_results(file_path: str) -> AnalyzeResult:
    with open(file_path, "rb") as f:
        poller = client.begin_analyze_document("prebuilt-layout",
                                               analyze_request=f,
                                               locale="en-US",
                                               content_type="application/octet-stream",
                                               features=[DocumentAnalysisFeature.BARCODES],
                                               output_content_format="markdown")
    return poller.result()

"""
if __name__ == '__main__':
    img1 = "image1.jpeg"
    doci1 = get_doc_int_results(img1)
    barcode = doci1.pages[0].barcodes[0].value if doci1.pages[0].barcodes else "n/a"
    oimg1 = analyze_with_gpt(img1, doci1.content, barcode)
    return(oimg1)
"""

def process_image(img_path):
  #Processes an image and returns the modified output.
  img1 = img_path
  doci1 = get_doc_int_results(img1)
  barcode = doci1.pages[0].barcodes[0].value if doci1.pages[0].barcodes else "n/a"
  oimg1 = analyze_with_gpt(img1, doci1.content, barcode)
  return(oimg1)
