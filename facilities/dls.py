from investigation_engine import InvestigationEngine
from investigation_io import JsonReader
from typing import List
import sys
import os
import logging

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class InvestigationDLS(InvestigationEngine):
        
    def __init__(self, json_path: str, investigation_id: str, output_path: str, transformation_json: str="./operations/dls/dls_metadata.json") -> None:
        logging.info("Instantiating DLS Investigation subclass")
        self.json_reader = JsonReader(json_path)
        self.operation_file_json = transformation_json
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        super().pre_run()


def dls_subparser(subparsers, parent_parser):
    parser_dls = subparsers.add_parser("dls", help="Parameter requirements for creating investigation files from DLS data", parents=[parent_parser])

    parser_dls.add_argument(
        "--json",
        help="Path to the .json file"
    )

def run(json_path : str, investigation_id: str, output_path: str) -> None:
    im = InvestigationDLS(json_path, investigation_id, output_path)
    im.pre_run()
    im.run()

def run_investigation_dls(args):
    if not args.dls_json:
        logging.error("DLS facility requires path to --dls-json file generated from ISYPB")
        return 1
    run(args.dls_json, args.investigation_id, args.output_folder,args.json)
