from abc import ABC, abstractmethod
from copy import deepcopy
from mmcif_gen.investigation_io import ExternalInformation, InvestigationStorage
import logging
import sys
from typing import Dict, List, TYPE_CHECKING

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class ItemDoNotExist(Exception):
    pass

class operationBase(ABC):
    def __init__(
        self, investigation_storage: InvestigationStorage, reader) -> None:
        self.investigation_storage = investigation_storage
        self.reader = reader

    @abstractmethod
    def perform_operation(sel, operation_data: dict) -> None:
        pass

    def check_items_exist_across_all_files(self, category, items):
        for item in items:
            if not self.reader.item_exists_across_all(category, item):
                return False
            elif self.reader.item_is_empty_in_any(category, item):
                return False
        return True

    def get_number_of_rows_in_data(self, data):
        items = list(data.keys())
        if not items:
            return 0
        total_rows = len(data[items[0]])
        return total_rows

    def filter_rows_by_items(self, rows, keys):
        filtered_dict = {key: rows[key] for key in keys if key in rows}
        return filtered_dict

    def reduce_rows_by_distinct_key(self, rows, distinct_keys):
        reduced_rows = {}
        distinct_values = set()
        items = list(rows.keys())
        total_rows = self.get_number_of_rows_in_data(rows)

        for index in range(total_rows):
            distinct_key = ""
            for key in distinct_keys:
                distinct_key = distinct_key + rows[key][index]
            if distinct_key in distinct_values:
                continue
            else:
                distinct_values.add(distinct_key)
                for item in items:
                    reduced_rows.setdefault(item, [])
                    reduced_rows[item].append(rows[item][index])
        return reduced_rows

    def remove_rows(self, rows, indices):
        indices.sort(reverse=True)
        items = list(rows.keys())
        total_rows = self.get_number_of_rows_in_data(rows)

        for index in indices:
            for item in items:
                del rows[item][index]
        return rows

    def evaluate_variable(self, value):
        if value[0] == "{":
            category, item = value[1:-1].split(".")
            return self.investigation_storage.data[category][item]
        else:
            return value

    def reduce_rows_by_condition(self, rows, condition_tag, condition_value):
        to_be_removed_indices = []
        condition_item = condition_tag.split(".")[-1]
        for index, item_value in enumerate(rows[condition_item]):
            if item_value not in condition_value:
                to_be_removed_indices.append(index)

        return self.remove_rows(rows, to_be_removed_indices)

    def rename_item(
        self, rows: Dict[str, List[str]], source: str, target: str
    ) -> Dict[str, List[str]]:
        rows[target] = rows.pop(source)
        return rows


class UnionOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing UnionOperation")

        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})

        distinct_keys = operation_parameters["primary_parameters"]
        key_exists = self.check_items_exist_across_all_files(
            source_category, distinct_keys
        )
        if not key_exists:
            distinct_keys = operation_parameters["secondary_parameters"]
        key_exists = self.check_items_exist_across_all_files(
            source_category, distinct_keys
        )
        if not key_exists:
            raise ItemDoNotExist

        collated_data = self.reader.collate_items(source_category, source_items)
        if target_items != "_same":
            for index, item in enumerate(target_items):
                self.rename_item(collated_data, source_items[index], item)
        self.investigation_storage.set_items(target_category, collated_data)

class EndpointOperation(operationBase):
    def perform_operation(self, operation_data: Dict) -> None:
        logging.info("Performing Endpoint Call")
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])

        operation_parameters = operation_data.get("operation_parameters", {})

        endpoint = operation_parameters["endpoint"]
        type = operation_parameters["type"]
        params = operation_parameters["params"]
        jq_filter = operation_parameters["jq"]
        if type == 'GET':
            resp = self.rest_reader.get(endpoint, params=params, filter_query=jq_filter)
        self.investigation_storage.data[target_category][target_items] = resp


class UnionDistinctOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing UnionOperation")

        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})

        distinct_keys = operation_parameters["primary_parameters"]
        key_exists = self.check_items_exist_across_all_files(
            source_category, distinct_keys
        )
        if not key_exists:
            distinct_keys = operation_parameters["secondary_parameters"]
        key_exists = self.check_items_exist_across_all_files(
            source_category, distinct_keys
        )
        if not key_exists:
            raise ItemDoNotExist

        collated_data = self.reader.collate_items(source_category, source_items)
        reduced_collated_data = self.reduce_rows_by_distinct_key(
            collated_data, distinct_keys
        )
        if target_items != "_same":
            for index, item in enumerate(target_items):
                self.rename_item(reduced_collated_data, source_items[index], item)
        self.investigation_storage.set_items(target_category, reduced_collated_data)


class AutoIncrementOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing AutoIncrementOperation")
        target_category = operation_data.get("target_category")
        target_item = operation_data.get("target_item")

        data = self.investigation_storage.data[target_category]
        rows = self.get_number_of_rows_in_data(data)
        self.investigation_storage.data[target_category][target_item] = []
        for i in range(rows):
            data[target_item].append(i + 1)


class StaticValueOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing StaticValue Operation")
        target_category = operation_data.get("target_category")
        target_items = operation_data.get("target_items")
        target_values = operation_data.get("target_values")

        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]
        rows_to_write = max(self.get_number_of_rows_in_data(data), 1)

        for index, item in enumerate(target_items):
            if item not in data:
                data[item] = []
            for i in range(rows_to_write):
                data[item].append(target_values[index])


class ModifyOperation(operationBase):
    # TODO: Change operation name to ModifyIntersection

    def perform_operation(self, operation_data):
        logging.info("Performing Union Modify Operation")
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", "")

        collated_data = self.reader.collate_item(source_category, source_items[0])
        same_data_across_files = collated_data.count(collated_data[0]) == len(
            collated_data
        )
        if not same_data_across_files:
            logging.info(
                "Values are not same across the files, Intersection Operation could not be performed"
            )
            # TODO: put a quesiton mark for the value:
        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]
        for index, item in enumerate(target_items):
            data[item] = [operation_parameters.format(collated_data[0])]


class IntersectionOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing IntersectionOperation")
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])

        collated_data = self.reader.collate_item(source_category, source_items[0])
        same_data_across_files = collated_data.count(collated_data[0]) == len(
            collated_data
        )
        if not same_data_across_files:
            logging.warning(
                "Values are not same across the files, Intersection Operation could not be performed"
            )
            # Put a "?" here as value
        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]
        for index, item in enumerate(target_items):
            data[item] = [collated_data[0]]


class ConditionalUnionOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing ConditionalUnion Operation")
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})

        conditional_tag = operation_parameters["conditional_variable"]
        conditional_values = operation_parameters["value"]

        conditional_item = conditional_tag.split(".")[-1]
        collation_items = deepcopy(source_items)
        if conditional_item not in source_items:
            collation_items.append(conditional_item)

        collated_data = self.reader.collate_items(source_category, collation_items)
        reduced_collated_data = self.reduce_rows_by_condition(
            collated_data, conditional_tag, conditional_values
        )

        for i in range(len(source_items)):
            reduced_collated_data = self.rename_item(
                reduced_collated_data, source_items[i], target_items[i]
            )

        filtered_data = self.filter_rows_by_items(reduced_collated_data, target_items)
        self.investigation_storage.set_items(target_category, filtered_data)


class ConditionalDistinctUnionOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing ConditionalUnion Operation")
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})

        conditional_tag = operation_parameters["conditional_variable"]
        conditional_values = operation_parameters["value"]

        conditional_item = conditional_tag.split(".")[-1]
        collation_items = deepcopy(source_items)
        if conditional_item not in source_items:
            collation_items.append(conditional_item)

        collated_data = self.reader.collate_items(source_category, collation_items)
        reduced_collated_data = self.reduce_rows_by_condition(
            collated_data, conditional_tag, conditional_values
        )
        reduced_collated_data = self.reduce_rows_by_distinct_key(
            reduced_collated_data, operation_parameters["distinct_key"]
        )

        for i in range(len(source_items)):
            reduced_collated_data = self.rename_item(
                reduced_collated_data, source_items[i], target_items[i]
            )

        filtered_data = self.filter_rows_by_items(reduced_collated_data, target_items)
        self.investigation_storage.set_items(target_category, filtered_data)


class CopyFillOperation(operationBase):
    # In copy, the data is copied to and from investigation file itself.
    # It copies the element of the first row of the source,
    # Writes it as many times as the target rows.

    def perform_operation(self, operation_data):
        logging.info("Performing CopyFill Operation")
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category")
        target_items = operation_data.get("target_items")

        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]
        rows_to_write = max(self.get_number_of_rows_in_data(data), 1)

        for index, item in enumerate(target_items):
            source_data = self.investigation_storage.get_item_data(
                source_category, source_items[index]
            )
            if item not in data:
                data[item] = []
            for i in range(rows_to_write):
                data[item].append(source_data[0])


class CopyConditionalModificationOperation(operationBase):
    # In copy, the data is copied to and from investigation file itself.
    # It checks for condition for each row of data with corresponding row of conditional variable
    # If value matches, copy the modified string
    # Else writes "?"

    def perform_operation(self, operation_data):
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category")
        target_items = operation_data.get("target_items")
        operation_parameters = operation_data.get("operation_parameters", {})
        value_to_check = operation_parameters["value"]
        modificaiton = operation_parameters["modification"]

        conditional_tag = operation_parameters["conditional_variable"]
        conditional_category, conditional_item = conditional_tag.split(".")

        data = self.investigation_storage.data[target_category]
        self.investigation_storage.add_category(target_category)

        for index, item in enumerate(target_items):
            source_data = self.investigation_storage.get_item_data(
                source_category, source_items[index]
            )
            conditional_data = self.investigation_storage.get_item_data(
                conditional_category, conditional_item
            )
            if item not in data:
                data[item] = []
            for index, row in enumerate(source_data):
                if conditional_data[index] == value_to_check:
                    data[item].append(modificaiton.format(row))
                else:
                    data[item].append("?")


class CopyOperation(operationBase):
    # In copy, the data is copied to and from investigation file itself.

    def perform_operation(self, operation_data):
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category")
        target_items = operation_data.get("target_items")

        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]

        for index, item in enumerate(target_items):
            source_data = self.investigation_storage.get_item_data(
                source_category, source_items[index]
            )
            data[item] = source_data


class CopyForEachRowOperation(operationBase):
    """
    Source is the model file unless specified
    Target is the investigation file

    It will check  value in  from the investigation file
    And find appropriate row it exists in the model file
    it will copy the source item at that index to the target.
    """

    def perform_operation(self, operation_data):
        logging.info("Performing CopyForEachRow Operation")
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})

        conditional_tag = operation_parameters["conditional_variable"]
        conditional_values = operation_parameters["value"]
        copy_from_investigation = operation_parameters.get(
            "source_is_investigation", ""
        )

        conditional_cat, conditional_item = conditional_tag.split(".")
        collation_items = deepcopy(source_items)

        if copy_from_investigation:
            source_data = self.investigation_storage.get_items_data(
                source_category, source_items
            )
        else:
            source_data = self.reader.collate_items(source_category, collation_items)

        repeats = operation_parameters.get("repeats", False)
        if copy_from_investigation:
            conditional_data = list(self.investigation_storage.get_items_data(
                conditional_cat, [conditional_item]
            ).values())[0]
        elif repeats:
            conditional_data = self.reader.collate_item_per_file(
                conditional_cat, conditional_item
            )
        else:
            conditional_data = self.reader.collate_item(
                conditional_cat, conditional_item
            )
        row_values = self.evaluate_variable(conditional_values)
        result = {}
        field_for_file_no = operation_parameters.get("model_file_id", False)
        if field_for_file_no:
            result.setdefault(field_for_file_no, [])

        for conditional_index, value in enumerate(row_values):
            for source_item in source_items:
                result.setdefault(source_item, [])
                if repeats:
                    for file_index, conditional in conditional_data.items():
                        if copy_from_investigation:
                            if value in conditional:
                                result[source_item].append(conditional_index + 1)
                                if field_for_file_no:
                                    result[field_for_file_no].append(file_index + 1)
                        else:
                            indices_of_value = [
                                i for i, x in enumerate(conditional) if x == value
                            ]
                            for index_of_value in indices_of_value:
                                result[source_item].append(
                                    source_data[source_item][index_of_value]
                                )
                                if field_for_file_no:
                                    result[field_for_file_no].append(file_index + 1)
                else:
                    index_of_value = conditional_data.index(value)
                    result[source_item].append(source_data[source_item][index_of_value])

        for i in range(len(source_items)):
            result = self.rename_item(result, source_items[i], target_items[i])

        if field_for_file_no:
            target_items.insert(0, field_for_file_no)
        filtered_data = self.filter_rows_by_items(result, target_items)
        self.investigation_storage.set_items(target_category, filtered_data)


class DeletionOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing Deletion Operation")
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])

        for item in target_items:
            self.investigation_storage.data[target_category].pop(item, None)


class ExternalInformationOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing External Information Operation")
        # Source category is from the investigation file.
        source_category = operation_data.get("source_category", "")
        source_items = operation_data.get("source_items", [])
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})
        filename = operation_parameters.get("file")

        external_inchi = ExternalInformation("./external_data/" + filename)
        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category].setdefault(
            target_items[0], []
        )

        items = self.investigation_storage.get_item_data(
            source_category, source_items[0]
        )
        for item in items:
            key = external_inchi.get_inchi_key(item)
            items = self.investigation_storage.data[target_category][
                target_items[0]
            ].append(key)


class SQLOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing SQL query Operation")
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})
        query = operation_parameters.get("query", "")

        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]

        response = self.reader.sql_execute(query)
        for result in response:
            for index, item in enumerate(target_items):
                data.setdefault(item, [])
                data[item].append(result[index])

class CopyFromPickleOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing CopyFromPickle Operation")
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        source_items = operation_data.get("source_items", [])

        self.investigation_storage.add_category(target_category)
        for index, item in enumerate(source_items):
            self.investigation_storage.data[target_category].setdefault(target_items[index], [])
            # Issues: semi-colons seperated multiple values. e.g.
            # 'primary_citation_author_name' ='Aschenbrenner, J.C.;Fairhead, M.;Godoy, A.S.;Balcomb, B.H.;Capkin, E.;Chandran, A.V.;Dolci, I.;Golding, M.;Koekemoer, L.;Lithgo, R.M.;
            
            if isinstance(self.reader[item], list):
                self.investigation_storage.data[target_category][target_items[index]].extend(self.reader[item])
            else:
                self.investigation_storage.data[target_category][target_items[index]].append(self.reader[item])

class NoopOperation(operationBase):
    def perform_operation(*args, **kwargs):
        pass

class JQFilterOperation(operationBase):
    def perform_operation(self, operation_data):
        logging.info("Performing JQ Filter Operation")
        target_category = operation_data.get("target_category", "")
        target_items = operation_data.get("target_items", [])
        operation_parameters = operation_data.get("operation_parameters", {})
        jq_filter = operation_parameters.get("jq", "")
        logging.info(f"Category: {target_category}, Item(s): {target_items}, JQ Filter: {jq_filter}")

        # Get filtered data from JSON reader
        filtered_data = self.reader.jq_filter(jq_filter)
        
        self.investigation_storage.add_category(target_category)
        data = self.investigation_storage.data[target_category]

        # Handle single target item case
        if isinstance(target_items, str):
            target_items = [target_items]
            if not isinstance(filtered_data, list):
                filtered_data = [filtered_data]
            data[target_items[0]] = filtered_data
            return

        # Handle multiple target items case
        if len(target_items) != len(filtered_data):
            raise ValueError(f"Number of target items ({len(target_items)}) does not match filtered data length ({len(filtered_data)})")
            
        for item, value in zip(target_items, filtered_data):
            if isinstance(value, list):
                data[item] = value
            else:
                data[item] = [value]
