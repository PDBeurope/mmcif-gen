from mmcif_gen.facilities.pdbe import pdbe_subparser, run_investigation_pdbe
from mmcif_gen.facilities.maxiv import maxiv_subparser, run_investigation_maxiv
from mmcif_gen.facilities.dls import dls_subparser, run_investigation_dls
from mmcif_gen.facilities.xchem import xchem_subparser, run_investigation_xchem
import argparse
import json
import logging
from logging.handlers import RotatingFileHandler

import os
import pathlib
import requests
import sys
from typing import Dict, List, Optional

file_handler = RotatingFileHandler('mmcifgen.log', maxBytes=100000, backupCount=3)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
file_handler.setLevel(logging.DEBUG)

logging.getLogger().addHandler(file_handler)

FACILITIES_URL = "https://raw.githubusercontent.com/PDBeurope/Investigations/main/mmcif_gen/operations/fetched_list.json"

class CLIManager:
    def __init__(self):
        self.facilities_data = None
        
    def fetch_facilities_data(self) -> Dict:
        """Fetch facilities data from GitHub."""
        if self.facilities_data is None:
            try:
                response = requests.get(FACILITIES_URL)
                response.raise_for_status()
                self.facilities_data = response.json()
            except requests.RequestException as e:
                print(f"Error fetching facilities data: {e}", file=sys.stderr)
                sys.exit(1)
        return self.facilities_data

    def get_available_facilities(self) -> List[str]:
        """Get list of available facilities."""
        return ['pdbe', 'maxiv', 'dls', 'xchem']
        # return list(self.fetch_facilities_data().keys())

    def get_facility_jsons(self, facility: str) -> List[str]:
        """Get available JSON files for a facility."""
        data = self.fetch_facilities_data()
        return data.get(facility, [])

    def fetch_facility_json(self, json_path: str, output_dir: str = ".") -> str:
        """Fetch a specific facility JSON file."""
        base_url = "https://raw.githubusercontent.com/PDBeurope/Investigations/main/mmcif_gen/"
        full_url = base_url + json_path
        
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.basename(json_path)
        output_path = os.path.join(output_dir, filename)
        
        print(f"Fetching {filename}...")
        try:
            response = requests.get(full_url)
            response.raise_for_status()
            
            with open(output_path, 'w') as f:
                json.dump(response.json(), f, indent=2)
            
            print(f"Successfully saved to {output_path}")
            return output_path
        except requests.RequestException as e:
            print(f"Error fetching JSON: {e}", file=sys.stderr)
            sys.exit(1)

    def find_local_json(self, facility: str) -> Optional[str]:
        """Find facility JSON in current directory."""
        possible_files = [
            f"{facility}_metadata.json",
            f"{facility}_metadata_hardcoded.json",
            f"{facility}_operations.json",
            f"{facility}_investigation.json"
        ]
        
        for file in possible_files:
            if os.path.exists(file):
                return file
        return None

def setup_parsers():
    parser = argparse.ArgumentParser(
        prog="mmcif-gen",
        description="Generate mmCIF files from various facility data sources"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Common arguments for make-mmcif command
    make_mmcif_parent = argparse.ArgumentParser(add_help=False)
    make_mmcif_parent.add_argument(
        "--json",
        help="Path to transformation JSON file"
    )
    make_mmcif_parent.add_argument(
        "--output-folder",
        help="Output folder for mmCIF files",
        default="./out"
    )
    make_mmcif_parent.add_argument(
        "--id",
        help="File identifier",
        default="I_1234"
    )

    # fetch-facility-json command
    fetch_parser = subparsers.add_parser(
        "fetch-facility-json",
        help="Fetch facility JSON configuration"
    )
    fetch_parser.add_argument(
        "json_name",
        help="Name of the JSON to fetch (e.g. dls-metadata)"
    )
    fetch_parser.add_argument(
        "-o", "--output-dir",
        help="Output directory",
        default="."
    )

    # make-mmcif command
    make_parser = subparsers.add_parser(
        "make-mmcif",
        help="Generate mmCIF file",
        parents=[make_mmcif_parent]
    )

    make_mmcif_subparser = make_parser.add_subparsers(
        dest="facility",
        help="Specifies facility for which mmcif files will be used for",
        required=True
    )
   
    # Create facility-specific parent parser
    facility_parent = argparse.ArgumentParser(add_help=False)
    
    # Add facility subparsers
    pdbe_subparser(make_mmcif_subparser, facility_parent)
    maxiv_subparser(make_mmcif_subparser, facility_parent)
    dls_subparser(make_mmcif_subparser, facility_parent)
    xchem_subparser(make_mmcif_subparser, facility_parent)

    return parser

def handle_command_error(parser: argparse.ArgumentParser, message: str):
    """Handle command errors with user-friendly messages."""
    print(f"Error: {message}", file=sys.stderr)
    parser.print_help()
    sys.exit(1)

def main():
    parser = setup_parsers()
    
    # Convert underscores to hyphens in command
    if len(sys.argv) > 1:
        sys.argv[1] = sys.argv[1].replace('_', '-')
    
    args = parser.parse_args()
    cli_manager = CLIManager()

    if not args.command:
        parser.print_help()
        print("\nAvailable facilities:", ', '.join(cli_manager.get_available_facilities()))
        sys.exit(1)

    # Check for local JSON first before any network operations
    if args.command == "make-mmcif":
        if not args.json:
            local_json = cli_manager.find_local_json(args.facility)
            if local_json:
                args.json = local_json
                print(f"Using local JSON file: {local_json}")

    if args.command == "fetch-facility-json":
        json_name = args.json_name.split('.')[0]
        available_jsons = []
        for facility, jsons in cli_manager.fetch_facilities_data().items():
            available_jsons.extend(jsons)

        available_jsons_pruned = [j.split('/')[-1].split('.')[0] for j in available_jsons] 
        if json_name not in available_jsons_pruned:
            print(f"No JSON found matching '{json_name}'")
            print("\nAvailable JSONs:")
            for json_path in available_jsons:
                print(f"  - {os.path.basename(json_path)}")
            sys.exit(1)

        index_of_match = available_jsons_pruned.index(json_name)
        cli_manager.fetch_facility_json(available_jsons[index_of_match], args.output_dir)

    elif args.command == "make-mmcif":
        available_facilities = cli_manager.get_available_facilities()
        
        if args.facility not in available_facilities:
            print(f"Invalid facility: {args.facility}")
            print(f"Available facilities: {', '.join(available_facilities)}")
            sys.exit(1)

        if not args.json:
            print("No JSON file specified and none found in current directory")
            print(f"\nAvailable JSONs for {args.facility}:")
            for json_path in cli_manager.get_facility_jsons(args.facility):
                print(f"  - {os.path.basename(json_path)}")
            print("\nFetch one using:")
            for json_path in cli_manager.get_facility_jsons(args.facility):
                base_name = os.path.splitext(os.path.basename(json_path))[0]
                print(f"  mmcif-gen fetch-facility-json {base_name}")
            sys.exit(1)

        # Create output directory
        os.makedirs(args.output_folder, exist_ok=True)
        print(f"Processing {args.facility} data using {args.json}...")

        if args.output_folder:
            pathlib.Path(args.output_folder).mkdir(parents=True, exist_ok=True) 
        if args.facility == 'pdbe':
            run_investigation_pdbe(args)
        elif args.facility == 'maxiv':
            run_investigation_maxiv(args)
        elif args.facility == 'dls':
            run_investigation_dls(args)
        elif args.facility == 'xchem':
            run_investigation_xchem(args)
        elif args.facility == 'esrf':
            run_investigation_esrf(args)

if __name__ == "__main__":
    main()