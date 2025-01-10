from mmcif_gen.investigation_io import InvestigationStorage
from mmcif_gen.operations import (
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
    EndpointOperation,
    CopyFromPickleOperation,
    JQFilterOperation
)
import json
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class InvestigationEngine:
    def __init__(self, investigation_id: str, output_path: str) -> None:
        self.investigation_storage = InvestigationStorage(investigation_id)
        self.output_path = output_path
        self.investigation_id = investigation_id
        self.operations = []

    def pre_run(self) -> None:
        self.read_json_operations()


    def read_json_operations(self) -> None:
        logging.info("Reading JSON operation files")
        with open(self.operation_file_json, "r") as file:
            json_data = json.load(file)
            self.operations = json_data.get("operations", [])
            self.investigation_storage.mmcif_order = json_data.get("mmcif_order", [])

    def operation_factory(self, operation_type: str, operation_reader: str) -> operationBase:
        if not operation_reader:
            operation_reader = self.reader
        elif operation_reader == "sqlite":
            operation_reader = self.sqlite_reader
        elif operation_reader == "pickle":
            operation_reader = self.pickle_reader
        elif operation_reader == "cif":
            operation_reader = self.reader
        elif operation_reader == "json":
            operation_reader = self.json_reader


        if operation_type == "distinct_union":
            return UnionDistinctOperation(self.investigation_storage, operation_reader)
        elif operation_type == "intersection":
            return IntersectionOperation(self.investigation_storage, operation_reader)
        elif operation_type == "auto_increment":
            return AutoIncrementOperation(self.investigation_storage, operation_reader)
        elif operation_type == "static_value":
            return StaticValueOperation(self.investigation_storage, operation_reader)
        elif operation_type == "modify_intersection":
            return ModifyOperation(self.investigation_storage, operation_reader)
        elif operation_type == "conditional_union":
            return ConditionalUnionOperation(self.investigation_storage, operation_reader)
        elif operation_type == "copy":
            return CopyOperation(self.investigation_storage, operation_reader)
        elif operation_type == "copy_fill":
            return CopyFillOperation(self.investigation_storage, operation_reader)
        elif operation_type == "copy_conditional_modify":
            return CopyConditionalModificationOperation(
                self.investigation_storage, operation_reader
            )
        elif operation_type == "copy_for_each_row":
            return CopyForEachRowOperation(self.investigation_storage, operation_reader)
        elif operation_type == "external_information":
            return ExternalInformationOperation(self.investigation_storage, operation_reader)
        elif operation_type == "deletion":
            return DeletionOperation(self.investigation_storage, operation_reader)
        elif operation_type == "conditional_distinct_union":
            return ConditionalDistinctUnionOperation(
                self.investigation_storage, operation_reader
            )
        elif operation_type == "sql_query":
            return SQLOperation(self.investigation_storage, operation_reader)
        elif operation_type == "rest_endpoint":
            return EndpointOperation(self.investigation_storage, None, operation_reader)
        elif operation_type == "noop":
            return NoopOperation(self.investigation_storage, operation_reader)
        elif operation_type == "copy_from_pickle":
            return CopyFromPickleOperation(self.investigation_storage, operation_reader)
        elif operation_type == "jq_filter":
            return JQFilterOperation(self.investigation_storage, operation_reader)
        else:
            raise ValueError(f"Invalid operation type: {operation_type}")

    def run(self) -> None :
        for operation_data in self.operations:
            try:
                operation_type = operation_data["operation"]
                operation_reader = operation_data.get("reader", None)
                operation = self.operation_factory(operation_type, operation_reader)
                operation.perform_operation(operation_data)
            except Exception as e:
                logging.error(f"Operation Failed:")
                logging.exception(e)

        self.investigation_storage.write_data_to_cif(
            f"{self.output_path}/{self.investigation_id}.cif"
        )
