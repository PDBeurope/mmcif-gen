from investigation_engine import InvestigationEngine
from typing import List
import sys
import logging
import argparse

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

class InvestigationMaxIV(InvestigationEngine):
        
    def __init__(self, sqlite_path: str, investigation_id: str, output_path: str) -> None:
        logging.info("Instantiating MaxIV Investigation subclass")
        self.sqlite_path = sqlite_path
        super().__init__(investigation_id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        super().pre_run()

def run(sqlite_path : str, investigation_id: str, output_path: str) -> None:
    im = InvestigationMaxIV(sqlite_path, investigation_id, output_path)
    im.pre_run()
    im.run()

    
def maxiv_subparser(subparsers, parent_parser):
    parser_maxiv = subparsers.add_parser("max_iv",help="Parameter requirements for investigation files from MAX IV data",parents=[parent_parser])
    parser_maxiv.add_argument(
        "-s",
        "--sqlite",
        help="Create investigation from set of pdb ids, space seperated",
    )

def run_investigation_maxiv(args):
    if not args.sqlite:
        logging.error("Max IV facility requires path to --sqlite file")
        return 1
    run(args.sqlite, args.investigation_id, args.output_path)


    