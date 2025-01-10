from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import RestReader
from typing import List
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class InvestigationESRF(InvestigationEngine):
        
    def __init__(self, user: str, pw:str, investigation_id: str, output_path: str) -> None:
        logging.info("Instantiating ESRF Investigation subclass")
        self.rest_reader = RestReader("https://htxlab.embl.org/", user, pw)
        self.rest_reader.get_auth_token()
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        super().pre_run()
    
def esrf_subparser(subparsers, parent_parser):
    parser_pdbe = subparsers.add_parser("esrf",help="Parameter requirements for investigation files from ESRF data", parents=[parent_parser])
    parser_pdbe.add_argument(
        "-f", 
        "--model-folder", help="Directory which contains model files"
    )
