from investigation_io import CIFReader, InvestigationStorage
from operations import (
    operationBase,
    IntersectionOperation,
    CopyOperation,
    CopyFillOperation,
    CopyConditionalModificationOperation,
    AutoIncrementOperation,
    ConditionalUnionOperation,
    StaticValueOperation,
    ModifyOperation,
    CopyForEachRowOperation,
    NoopOperation,
    DeletionOperation,
    ExternalInformationOperation,
    ConditionalDistinctUnionOperation,
    UnionDistinctOperation,
    SQLOperation,
)
from typing import List
import json
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class InvestigationEngine:
    def __init__(self, model_file_path: List[str], investigation_id: str, output_path: str) -> None:
        self.reader = CIFReader()
        self.investigation_storage = InvestigationStorage()
        self.model_file_path = model_file_path
        self.operation_file_json = "./operations.json"
        self.output_path = output_path
        self.investigation_id = investigation_id
        self.operations = []

    def pre_run(self) -> None:
        logging.info("Pre-running")
        self.reader.read_files(self.model_file_path)
        self.reader.create_denormalised_tables()
        self.reader.build_denormalised_data()
        self.reader.add_struct_ref_data()
        self.reader.add_descript_categories()
        self.reader.add_sample_category()
        self.reader.add_synchrotron_data()
        self.reader.add_exptl_data()
        self.reader.add_investigation_id(self.investigation_id)
        self.read_json_operations()

    def read_json_operations(self) -> None:
        logging.info("Reading JSON operation files")
        with open(self.operation_file_json, "r") as file:
            json_data = json.load(file)
            self.operations = json_data.get("operations", [])
            self.investigation_storage.mmcif_order = json_data.get("mmcif_order", [])

    def operation_factory(self, operation_type: str) -> operationBase:
        if operation_type == "distinct_union":
            return UnionDistinctOperation(self.investigation_storage, self.reader)
        elif operation_type == "intersection":
            return IntersectionOperation(self.investigation_storage, self.reader)
        elif operation_type == "auto_increment":
            return AutoIncrementOperation(self.investigation_storage, self.reader)
        elif operation_type == "static_value":
            return StaticValueOperation(self.investigation_storage, self.reader)
        elif operation_type == "modify_intersection":
            return ModifyOperation(self.investigation_storage, self.reader)
        elif operation_type == "conditional_union":
            return ConditionalUnionOperation(self.investigation_storage, self.reader)
        elif operation_type == "copy":
            return CopyOperation(self.investigation_storage, self.reader)
        elif operation_type == "copy_fill":
            return CopyFillOperation(self.investigation_storage, self.reader)
        elif operation_type == "copy_conditional_modify":
            return CopyConditionalModificationOperation(
                self.investigation_storage, self.reader
            )
        elif operation_type == "copy_for_each_row":
            return CopyForEachRowOperation(self.investigation_storage, self.reader)
        elif operation_type == "external_information":
            return ExternalInformationOperation(self.investigation_storage, self.reader)
        elif operation_type == "deletion":
            return DeletionOperation(self.investigation_storage, self.reader)
        elif operation_type == "conditional_distinct_union":
            return ConditionalDistinctUnionOperation(
                self.investigation_storage, self.reader
            )
        elif operation_type == "sql_query":
            return SQLOperation(self.investigation_storage, self.reader)
        elif operation_type == "noop":
            return NoopOperation(self.investigation_storage, self.reader)
        else:
            raise ValueError(f"Invalid operation type: {operation_type}")

    def run(self) -> None :
        for operation_data in self.operations:
            try:
                operation_type = operation_data.get("operation", "")
                operation = self.operation_factory(operation_type)
                operation.perform_operation(operation_data)
            except Exception as e:
                logging.error(f"Operation Failed:")
                logging.error(json.dumps(operation_data))

        self.investigation_storage.write_data_to_cif(
            f"{self.output_path}/{self.investigation_id}.cif"
        )
