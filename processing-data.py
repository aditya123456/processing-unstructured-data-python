# Warning control
import warnings
warnings.filterwarnings('ignore')
from IPython.display import JSON
from IPython.display import Image
import json
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError
from unstructured.partition.html import partition_html
from unstructured.partition.pptx import partition_pptx
from unstructured.staging.base import dict_to_elements, elements_to_json
from unstructured.chunking.basic import chunk_elements
from unstructured.chunking.title import chunk_by_title
from unstructured.staging.base import dict_to_elements
from Utils import Utils


class ProcessData:
    utils = Utils()

    def __init__(self):
        self.DLAI_API_KEY = self.utils.get_dlai_api_key()
        self.DLAI_API_URL = self.utils.get_dlai_url()

        self.s = UnstructuredClient(
            api_key_auth=self.DLAI_API_KEY,
            server_url=self.DLAI_API_URL,
        )

    def html_file_process(self):

        Image(filename="images/HTML_demo.png", height=600, width=600)

        filename = "example_files/medium_blog.html"
        elements = partition_html(filename=filename)

        element_dict = [el.to_dict() for el in elements]
        example_output = json.dumps(element_dict[11:15], indent=2)
        print(example_output)
        JSON(example_output)


    def pptx_file_process(self):
        Image(filename="images/pptx_slide.png", height=600, width=600)

        filename = "example_files/msft_openai.pptx"
        elements = partition_pptx(filename=filename)

        element_dict = [el.to_dict() for el in elements]
        JSON(json.dumps(element_dict[:], indent=2))

    def pdf_file_process(self):
        filename = "example_files/CoT.pdf"
        with open(filename, "rb") as f:
            files = shared.Files(
                content=f.read(),
                file_name=filename,
            )

        req = shared.PartitionParameters(
            files=files,
            strategy='hi_res',
            pdf_infer_table_structure=True,
            languages=["eng"],
        )
        try:
            resp = self.s.general.partition(req)
            print(json.dumps(resp.elements[:3], indent=2))
        except SDKError as e:
            print(e)


    def metadata_chunking_extraction(self):
        Image(filename='images/winter-sports-cover.png', height=400, width=400)
        Image(filename="images/winter-sports-toc.png", height=400, width=400)

        filename = "example_files/winter-sports.epub"

        with open(filename, "rb") as f:
            files = shared.Files(
                content=f.read(),
                file_name=filename,
            )

        req = shared.PartitionParameters(files=files)

        try:
            resp = self.s.general.partition(req)
        except SDKError as e:
            print(e)

        JSON(json.dumps(resp.elements[0:3], indent=2))

        [x for x in resp.elements if x['type'] == 'Title' and 'hockey' in x['text'].lower()]

        chapters = [
            "THE SUN-SEEKER",
            "RINKS AND SKATERS",
            "TEES AND CRAMPITS",
            "ICE-HOCKEY",
            "SKI-ING",
            "NOTES ON WINTER RESORTS",
            "FOR PARENTS AND GUARDIANS",
        ]

        chapter_ids = {}
        for element in resp.elements:
            for chapter in chapters:
                if element["text"] == chapter and element["type"] == "Title":
                    chapter_ids[element["element_id"]] = chapter
                    break

        chapter_to_id = {v: k for k, v in chapter_ids.items()}
        [x for x in resp.elements if x["metadata"].get("parent_id") == chapter_to_id["ICE-HOCKEY"]][0]

        client = chromadb.PersistentClient(path="chroma_tmp", settings=chromadb.Settings(allow_reset=True))
        client.reset()

        collection = client.create_collection(
            name="winter_sports",
            metadata={"hnsw:space": "cosine"}
        )

        for element in resp.elements:
            parent_id = element["metadata"].get("parent_id")
            chapter = chapter_ids.get(parent_id, "")
            collection.add(
                documents=[element["text"]],
                ids=[element["element_id"]],
                metadatas=[{"chapter": chapter}]
            )

        results = collection.peek()
        print(results["documents"])

        result = collection.query(
            query_texts=["How many players are on a team?"],
            n_results=2,
            where={"chapter": "ICE-HOCKEY"},
        )
        print(json.dumps(result, indent=2))

        elements = dict_to_elements(resp.elements)

        chunks = chunk_by_title(
            elements,
            combine_text_under_n_chars=100,
            max_characters=3000,
        )

        JSON(json.dumps(chunks[0].to_dict(), indent=2))

