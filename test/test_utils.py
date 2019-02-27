import unittest

from shuttle.utils import Doc


class TestDoc(unittest.TestCase):

    def test_init_with_dict(self):
        doc = Doc(dict(my='wonderful', dictionary=dict(is_a_test=True)))
        self.assertEqual(doc['my'], 'wonderful')
        self.assertEqual(doc.get('dictionary.is_a_test'), True)
        self.assertEqual(doc['dictionary']['is_a_test'], True)

    def test_init_with_kwargs(self):
        doc = Doc(my='wonderful', dictionary=dict(is_a_test=True))
        self.assertEqual(doc['my'], 'wonderful')
        self.assertEqual(doc.get('dictionary.is_a_test'), True)
        self.assertEqual(doc['dictionary']['is_a_test'], True)

    def test_behaves_as_a_dict(self):
        doc = Doc(this=dict(is_a=dict(nested='structure')))

        with self.assertRaises(KeyError):
            doc['this.is_a.nested']

        with self.assertRaises(KeyError):
            doc['this.does.not.exist']

        self.assertEqual(doc['this']['is_a']['nested'], 'structure')

    def test_returns_none_if_no_key(self):
        doc = Doc(this=dict(is_a=dict(nested='structure')))
        self.assertIsNone(doc.get('this.does.not.exist'))

    def test_traverses_many_levels(self):
        doc = Doc(this=dict(is_a=dict(nested='structure')))
        self.assertEqual(doc.get('this.is_a.nested'), 'structure')
