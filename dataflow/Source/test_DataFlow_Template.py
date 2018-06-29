import unittest
import DataFlow_Template
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class DataflowTest(unittest.TestCase):

    testCollection = [{'PrevRefIPs': '12', 'TLD': 'au', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                           'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8',
                           'GlobalRank': '1', 'PrevRefSubNets': '11'},
                          {'PrevRefIPs': '12', 'TLD': 'com', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                           'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8',
                           'GlobalRank': '1', 'PrevRefSubNets': '11'},
                          {'PrevRefIPs': '12', 'TLD': 'au', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                           'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8',
                           'GlobalRank': '1', 'PrevRefSubNets': '11'}]


    def test_Split_shouldReturnRecordFromLine(self):
        SCHEMA = 'GlobalRank:INTEGER,TldRank:INTEGER,Domain:STRING,TLD:STRING,RefSubNets:INTEGER,RefIPs:INTEGER,IDN_Domain:STRING,' \
                 'IDN_TLD:STRING,PrevGlobalRank:INTEGER,PrevTldRank:INTEGER,PrevRefSubNets:INTEGER,PrevRefIPs:INTEGER'
        split = DataFlow_Template.Split()
        expected = {'PrevRefIPs': '12', 'TLD': '4', 'IDN_Domain': '7', 'PrevGlobalRank': '9', 'TldRank': '2',
                          'Domain': '3', 'PrevTldRank': '10', 'RefIPs': '6', 'RefSubNets': '5', 'IDN_TLD': '8', 'GlobalRank': '1', 'PrevRefSubNets': '11'}
        result = split.process("1,2,3,4,5,6,7,8,9,10,11,12", SCHEMA)[0]
        self.assertDictEqual(expected, result)

    def test_CountTLDs_shouldReturnFilteredTLDsWithCount(self):

        with TestPipeline() as p:
            testPCollection = (p | beam.Create(self.testCollection))
            excludes = (p | 'exclude' >> beam.Create(['com', 'net']))
            countTLDS = DataFlow_Template.CountTLDs(excludes)
            expected = [{'Count': 2, 'TLD': 'au'}]
            result = countTLDS.expand(testPCollection)
            assert_that(result, equal_to(expected))


    def test_AddDTLDDesc_shouldAddDescriptionToElement(self):

        expected = {'Domain': '3',
          'GlobalRank': '1',
          'IDN_Domain': '7',
          'IDN_TLD': '8',
          'PrevGlobalRank': '9',
          'PrevRefIPs': '12',
          'PrevRefSubNets': '11',
          'PrevTldRank': '10',
          'RefIPs': '6',
          'RefSubNets': '5',
          'TLD': 'au',
          'TLD_Desc': 'Australia',
          'TldRank': '2'}
        

        testInstance = DataFlow_Template.AddDTLDDesc()
        result = testInstance.process(self.testCollection[0], { 'au' : 'Australia', 'us' : 'United States'})
        self.assertDictEqual(expected, result[0])

def main():
    unittest.main()


if __name__ == '__main__':
    unittest.main()