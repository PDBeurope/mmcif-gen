from mmcif_gen.investigation_engine import InvestigationEngine
from mmcif_gen.investigation_io import JsonReader
from typing import List
import gemmi
import os
import sys
import logging
import tempfile

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class InvestigationCrims(InvestigationEngine):
        
    def __init__(self, json_path: str, id: str, output_path: str, transformation_json: str="./operations/crims/crims_metadata.json", merge_file: str = None) -> None:
        logging.info("Instantiating Crims subclass")
        logging.info(f"Creating file id: {id}")
        self.json_reader = JsonReader(json_path)
        self.operation_file_json = transformation_json
        self.merge_file = merge_file
        super().__init__(id, output_path)

    def pre_run(self) -> None:
        logging.info("Pre-running")
        super().pre_run()

    def write_output(self, prefer_pairs: bool = False) -> None:
        if not self.merge_file:
            super().write_output(prefer_pairs)
            return
        self._merge_generated_categories()

    def _merge_generated_categories(self) -> None:
        if not os.path.isfile(self.merge_file):
            raise FileNotFoundError(f"CIF file to merge into does not exist: {self.merge_file}")

        generated_file = tempfile.NamedTemporaryFile(suffix=".cif", delete=False)
        generated_path = generated_file.name
        generated_file.close()
        try:
            self.investigation_storage.write_data_to_cif(generated_path, prefer_pairs=False)
            self._copy_categories_onto_existing_cif(generated_path)
        finally:
            if os.path.exists(generated_path):
                os.remove(generated_path)

    def _copy_categories_onto_existing_cif(self, generated_path: str) -> None:
        logging.info(f"Merging generated categories into a copy of {self.merge_file}")
        base = gemmi.cif.read(self.merge_file)
        try:
            block = base.sole_block()
        except RuntimeError as error:
            raise ValueError(
                f"{self.merge_file} must contain exactly one data block to merge into"
            ) from error

        generated = gemmi.cif.read(generated_path).sole_block()
        existing_categories = set(block.get_mmcif_category_names())
        for category in generated.get_mmcif_category_names():
            if category in existing_categories:
                logging.warning(
                    f"Replacing category {category} already present in {self.merge_file}"
                )
            table = generated.get_mmcif_category(category)
            block.set_mmcif_category(category, {item: list(values) for item, values in table.items()})

        output_file = self.get_output_file_name()
        write_options = gemmi.cif.WriteOptions()
        write_options.align_loops = 50
        write_options.align_pairs = 50
        block.write_file(output_file, write_options)
        logging.info(f"Writing cif file: {output_file}")


def crims_subparser(subparsers, parent_parser):
    parser_crimms = subparsers.add_parser("crims", help="Parameter requirements for creating mmcif files from CRIMS data", parents=[parent_parser])

    parser_crimms.add_argument(
        "--crims-json",
        help="Path to the .json file generated from CRIMS microservice endpoint"
    )
    parser_crimms.add_argument(
        "--merge-file",
        help=(
            "Existing mmCIF to merge generated categories into. "
            "Categories produced by the mapping replace the same categories in this file; "
            "every other category is kept. The original file is not modified. "
            "The merged file is written to the output folder."
        ),
    )

def run(crims_json_path : str, id: str, output_path: str, operation_json_path: str, merge_file: str = None) -> None:
    im = InvestigationCrims(crims_json_path, id, output_path, operation_json_path, merge_file=merge_file)
    im.pre_run()
    im.run(prefer_pairs=True)

def run_investigation_crims(args):
    if not args.crims_json:
        logging.error("Crims facility requires path to --crims-json file generated from microservice endpoint")
        return 1
    if args.merge_file and not os.path.isfile(args.merge_file):
        logging.error(f"CIF file to merge into does not exist: {args.merge_file}")
        return 1
    run(args.crims_json, args.id, args.output_folder, args.json, args.merge_file)
