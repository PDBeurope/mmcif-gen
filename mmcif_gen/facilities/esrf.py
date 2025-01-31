from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import RestReader
from typing import List
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class InvestigationESRF(InvestigationEngine):
        
    def __init__(self, esrf_json_path: str, investigation_id: str, output_path: str) -> None:
        logging.info("Instantiating ESRF Investigation subclass")
        # self.rest_reader = RestReader("https://htxlab.embl.org/", user, pw)
        # self.rest_reader.get_auth_token()
        self.operation_file_json = "./operations/esrf/esrf_investigation.json"
        self.json_reader = JsonReader(esrf_json_path)
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        super().pre_run()

def run(esrf_json_path : str, id: str, output_path: str, operation_json_path: str) -> None:
    im = InvestigationESRF(esrf_json_path, id, output_path, operation_json_path)
    im.pre_run()
    im.run() 

def run_investigation_esrf(args):
    if not args.esrf_json:
        logging.error("ESRF facility requires path to --esrf-json file generated from CRIMMS")
        return 1
    run(args.esrf_json, args.id, args.output_folder,args.json)   

def esrf_subparser(subparsers, parent_parser):
    parser_pdbe = subparsers.add_parser("esrf",help="Parameter requirements for investigation files from ESRF data", parents=[parent_parser])
    parser_pdbe.add_argument(
        "-f", 
        "--esrf-json", help="Path to the json file created from CRIMMS microservice"
    )

