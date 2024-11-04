from facilities.pdbe import pdbe_subparser, run_investigation_pdbe
from facilities.maxiv import maxiv_subparser, run_investigation_maxiv
from facilities.dls import dls_subparser, run_investigation_dls
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
        description="This application creates an mmCIF file from provided data sources.\
             The application is facility oriented, and requires data based on the definition in the facility specific subparser"    )
   
    subparsers = parser.add_subparsers(help="Specifies facility for which investigation files will be used for", 
                                       dest="facility",
                                       )
    pdbe_subparser(subparsers, parent_parser)
    maxiv_subparser(subparsers, parent_parser)
    esrf_subparser(subparsers, parent_parser)
    dls_subparser(subparsers, parent_parser)
    args = parser.parse_args()

    if args.output_folder:
        pathlib.Path(args.output_folder).mkdir(parents=True, exist_ok=True) 
    if args.facility == 'pdbe':
        run_investigation_pdbe(args)
    elif args.facility == 'max_iv':
        run_investigation_maxiv(args)
    elif args.facility == 'dls':
        run_investigation_dls(args)

if __name__ == "__main__":
    main()