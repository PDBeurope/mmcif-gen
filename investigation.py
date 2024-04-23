from facilities.pdbe import pdbe_subparser, run_investigation_pdbe
from facilities.maxiv import maxiv_subparser, run_investigation_maxiv
from facilities.esrf import  esrf_subparser
import argparse
import logging
import sys
import pathlib

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def main() -> None:
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "-o",
        "--output-folder",
        help="Folder to output the created investigation files to",
        default="./out",
    )
    parent_parser.add_argument(
        "-i",
        "--investigation-id",
        help="Investigation ID to assign to the resulting investigation file",
        default="I_1234",
    )

    parser = argparse.ArgumentParser(
        prog="Investigation",
        description="This creates an investigation file from a collection of model files\
             which can be provided as folder path, pdb_ids, or a csv file. The model files can be provided"    )
   
    subparsers = parser.add_subparsers(help="Specifies facility for which investigation files will be used for", 
                                       dest="facility",
                                       )
    pdbe_subparser(subparsers, parent_parser)
    maxiv_subparser(subparsers, parent_parser)
    esrf_subparser(subparsers, parent_parser)
    args = parser.parse_args()

    if args.output_folder:
        pathlib.Path(args.output_folder).mkdir(parents=True, exist_ok=True) 
    if args.facility == 'pdbe':
        run_investigation_pdbe(args)
    elif args.facility == 'Max IV':
        run_investigation_maxiv(args)

if __name__ == "__main__":
    main()