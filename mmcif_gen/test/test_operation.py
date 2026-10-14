import unittest
from unittest.mock import MagicMock, patch
from mmcif_gen.operations import (
    UnionOperation,
    UnionDistinctOperation,
    AutoIncrementOperation,
    StaticValueOperation,
    ModifyOperation,
    IntersectionOperation,
    ConditionalUnionOperation,
    ConditionalDistinctUnionOperation,
    CopyFillOperation,
    CopyConditionalModificationOperation,
    CopyOperation,
    CopyForEachRowOperation,
    DeletionOperation,
    ExternalInformationOperation,
    SQLOperation,
    NoopOperation,
    ItemDoNotExist
)
from investigation_io import InvestigationStorage, CIFReader
import os


class TestOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dummy1_cif_path = 'test/data/dummy1.cif'
        cls.dummy2_cif_path = 'test/data/dummy2.cif'

    def setUp(self):
        self.investigation_storage = InvestigationStorage("test_investigation")
        self.reader = CIFReader()
        self.mock_storage = MagicMock(spec=InvestigationStorage)
        self.mock_reader = CIFReader()
        self.mock_reader.read_files([self.dummy1_cif_path, self.dummy2_cif_path])
        # Ensure mock_storage has a data attribute that is a dictionary
        self.mock_storage.data = {}

    def test_union_operation(self):
        operation = UnionOperation(self.mock_storage, self.mock_reader)
        operation_data = {
            "source_category": "_audit_author",
            "source_items": ["name", "pdbx_ordinal"],
            "target_category": "_target",
            "target_items": ["name", "pdbx_ordinal"],
            "operation_parameters": {"primary_parameters": ["name"], "secondary_parameters": ["pdbx_ordinal"]}
        }
        operation.perform_operation(operation_data)
        self.mock_storage.set_items.assert_called_with(
            "_target", 
            {"name": ["Snee, M.", "Talon, R.", "Fowley, D.", "Name something","Snee, M.", "Talon, R.", "Fowley, D."], 
             "pdbx_ordinal": ["1", "2", "3", "4","1", "2", "3"]}
        )

    def test_union_distinct_operation(self):
        operation = UnionDistinctOperation(self.mock_storage, self.mock_reader)
        operation_data = {
            "source_category": "_audit_author",
            "source_items": ["name", "pdbx_ordinal"],
            "target_category": "_target",
            "target_items": ["name", "pdbx_ordinal"],
            "operation_parameters": {"primary_parameters": ["name"], "secondary_parameters": ["pdbx_ordinal"]}
        }
        operation.perform_operation(operation_data)
        self.mock_storage.set_items.assert_called_with(
            "_target", 
            {"name": ["Snee, M.", "Talon, R.", "Fowley, D.", "Name something"], 
             "pdbx_ordinal": ["1", "2", "3", "4"]}
        )

    def test_auto_increment_operation(self):
        operation = AutoIncrementOperation(self.mock_storage, self.mock_reader)
        self.mock_storage.data = {"category": {"item": ["data1", "data2"]}}
        operation_data = {
            "target_category": "category",
            "target_item": "incremented_item"
        }
        operation.perform_operation(operation_data)
        self.assertEqual(self.mock_storage.data["category"]["incremented_item"], [1, 2])

    def test_static_value_operation(self):
        operation = StaticValueOperation(self.mock_storage, self.mock_reader)
        self.mock_storage.data = {"category": {}}
        operation_data = {
            "target_category": "category",
            "target_items": ["item1", "item2"],
            "target_values": ["value1", "value2"]
        }
        operation.perform_operation(operation_data)
        self.assertEqual(self.mock_storage.data["category"]["item1"], ["value1"])
        self.assertEqual(self.mock_storage.data["category"]["item2"], ["value2"])

    def test_modify_operation(self):
        operation = ModifyOperation(self.mock_storage, self.mock_reader)
        # self.mock_reader.collate_item.return_value = ["data"]
        operation_data = {
            "source_category": "_struct",
            "source_items": ["entry_id"],
            "target_category": "target",
            "target_items": ["doi"],
            "operation_parameters": "pdb_{}"
        }
        self.mock_storage.data["target"] = {}
        operation.perform_operation(operation_data)
        self.mock_storage.add_category.assert_called_with("target")
        self.assertEqual(self.mock_storage.data["target"]["doi"], ["pdb_1CBS"])

    @unittest.skip("need to verify whether failure is an implementaiton issue")
    def test_intersection_operation(self):
        operation = IntersectionOperation(self.mock_storage, self.mock_reader)
        operation_data = {
            "source_category": "_audit_author",
            "source_items": ["name"],
            "target_category": "target",
            "target_items": ["different_author"]
        }
        self.mock_storage.data["target"] = {}
        operation.perform_operation(operation_data)
        self.mock_storage.add_category.assert_called_with("target")
        self.assertEqual(self.mock_storage.data["target"]["different_author"], ["Name something"])

    def test_conditional_union_operation(self):
        operation = ConditionalUnionOperation(self.mock_storage, self.mock_reader)
        operation_data = {
            "source_category": "_audit_author",
            "source_items": ["name"],
            "target_category": "target",
            "target_items": ["name"],
            "operation_parameters": {
                "conditional_variable": "_audit_author.pdbx_ordinal",
                "value": ["4"]
            }
        }
        operation.perform_operation(operation_data)
        self.mock_storage.set_items.assert_called_with("target", {"name": ["Name something"]})

    def test_conditional_distinct_union_operation(self):
        operation = ConditionalDistinctUnionOperation(self.mock_storage, self.mock_reader)
        operation_data = {
            "source_category": "_audit_author",
            "source_items": ["name"],
            "target_category": "target",
            "target_items": ["name"],
            "operation_parameters": {
                "conditional_variable": "_audit_author.pdbx_ordinal",
                "value": ["1"],
                "distinct_key": ["name"]
            }
        }
        operation.perform_operation(operation_data)
        self.mock_storage.set_items.assert_called_with("target", {"name": ["Snee, M."]})

    def test_copy_fill_operation(self):
        operation = CopyFillOperation(self.investigation_storage, self.mock_reader)
        self.investigation_storage.data = {"target": {"existing_item": ["row1", "row2"]},
                                  "source": {"copy_me": ["1cbs"]}}
        operation_data = {
            "source_category": "source",
            "source_items": ["copy_me"],
            "target_category": "target",
            "target_items": ["code"]
        }
        operation.perform_operation(operation_data)
        self.assertEqual(self.investigation_storage.data["target"]["code"], ["1cbs", "1cbs"])

    def test_copy_conditional_modification_operation(self):
        operation = CopyConditionalModificationOperation(self.investigation_storage, self.mock_reader)
        self.investigation_storage.data = {
            "target": {},
            "source": { "item": ["data1", "data2"],
                        "condition": ["True", "False"]}
        }
        operation_data = {
            "source_category": "source",
            "source_items": ["item"],
            "target_category": "target",
            "target_items": ["item"],
            "operation_parameters": {
                "conditional_variable": "source.condition",
                "value": "True",
                "modification": "modified_{}"
            }
        }
        operation.perform_operation(operation_data)
        self.assertEqual(self.investigation_storage.data["target"]["item"], ["modified_data1", "?"])

    def test_copy_operation(self):
        operation = CopyOperation(self.investigation_storage, self.mock_reader)
        self.investigation_storage.data = {"target": {},
                                           "source": {"item" : ["value1", "value2"]}}
        operation_data = {
            "source_category": "source",
            "source_items": ["item"],
            "target_category": "target",
            "target_items": ["item"]
        }
        operation.perform_operation(operation_data)
        self.assertEqual(self.investigation_storage.data["target"]["item"], ["value1", "value2"])

    def test_copy_for_each_row_operation(self):
        operation = CopyForEachRowOperation(self.investigation_storage, self.mock_reader)
        self.investigation_storage.data = {"target": {"existing_ordinal": ["1", "3"]}}
        
        operation_data = {
            "source_category": "_audit_author",
            "source_items": ["name"],
            "target_category": "target",
            "target_items": ["name"],
            "operation_parameters": {
                "conditional_variable": "_audit_author.pdbx_ordinal",
                "value": "{target.existing_ordinal}"
            }
        }
        operation.perform_operation(operation_data)
        self.assertEqual(self.investigation_storage.data["target"]["name"], ["Snee, M.", 'Fowley, D.'])

    def test_deletion_operation(self):
        operation = DeletionOperation(self.mock_storage, self.mock_reader)
        self.mock_storage.data = {"target": {"item": ["data"]}}
        operation_data = {
            "target_category": "target",
            "target_items": ["item"]
        }
        operation.perform_operation(operation_data)
        self.assertNotIn("item", self.mock_storage.data["target"])

    @unittest.skip("Need to correct this unit test")
    def test_external_information_operation(self):
        operation = ExternalInformationOperation(self.mock_storage, self.mock_reader)
        self.mock_storage.data = {"source": {"item": ["data"]}, "target": {}}
        operation_data = {
            "source_category": "source",
            "source_items": ["item"],
            "target_category": "target",
            "target_items": ["item"],
            "operation_parameters": {"file": "external.json"}
        }
        with patch('operations.ExternalInformation') as MockExternal:
            mock_external = MockExternal.return_value
            mock_external.get_inchi_key.return_value = "inchi_key"
            operation.perform_operation(operation_data)
            self.assertEqual(self.mock_storage.data["target"]["item"], ["inchi_key"])

if __name__ == '__main__':
    unittest.main()
