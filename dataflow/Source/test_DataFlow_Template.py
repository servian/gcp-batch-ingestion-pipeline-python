import unittest
import DataFlow_Template;

class DataflowTest(unittest.TestCase):

    def test_Split_shouldReturnObjectFromLine(self):
        split = DataFlow_Template.Split()
        expected = {'PrevRefIPs': '12', 'TLD': '4', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                          'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8', 'GlobalRank': '1', 'PrevRefSubNets': '11'}
        result = split.process("1,2,3,4,5,6,7,8,9,10,11,12")[0]
        self.assertDictEqual(expected, result)

    def test_CountTLDs_shouldReturnObjectFromLine(self):
        testCollection = [{'PrevRefIPs': '12', 'TLD': 'au', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                           'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8',
                           'GlobalRank': '1', 'PrevRefSubNets': '11'},
                          {'PrevRefIPs': '12', 'TLD': 'com', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                           'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8',
                           'GlobalRank': '1', 'PrevRefSubNets': '11'},
                          {'PrevRefIPs': '12', 'TLD': 'au', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                           'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8',
                           'GlobalRank': '1', 'PrevRefSubNets': '11'}]

        split = DataFlow_Template.CountTLDs()
        expected = [{'Count': 2, 'TLD': 'au'}, {'Count': 1, 'TLD': 'com'}]
        result = split.expand(testCollection)
        self.assertEqual(expected, result)

def main():
    unittest.main()


if __name__ == '__main__':
    unittest.main()