from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import JsonReader
from typing import List
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class InvestigationCrims(InvestigationEngine):
        
    def __init__(self, json_path: str, id: str, output_path: str, transformation_json: str="./operations/crims/crims_metadata.json") -> None:
        logging.info("Instantiating Crims subclass")
        logging.info(f"Creating file id: {id}")
        self.json_reader = JsonReader(json_path)
        self.operation_file_json = transformation_json
        super().__init__(id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        super().pre_run()


def crims_subparser(subparsers, parent_parser):
    parser_crimms = subparsers.add_parser("crims", help="Parameter requirements for creating mmcif files from CRIMS data", parents=[parent_parser])

    parser_crimms.add_argument(
        "--crims-json",
        help="Path to the .json file generated from CRIMS microservice endpoint"
    )

def run(crims_json_path : str, id: str, output_path: str, operation_json_path: str) -> None:
    im = InvestigationCrims(crims_json_path, id, output_path, operation_json_path)
    im.pre_run()
    im.run(prefer_pairs=True)

def run_investigation_crims(args):
    if not args.crims_json:
        logging.error("Crims facility requires path to --crims-json file generated from microservice endpoint")
        return 1
    run(args.crims_json, args.id, args.output_folder,args.json)
