import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class CleanMeasurementsDoFn(beam.DoFn):

  def process(self, element):
    record = json.loads(element.decode('utf-8'))

    if any(record.get(field) is None for field in ['time', 'profileName', 'temperature', 'humidity', 'pressure']):
      return

    try:
      tempC = float(record['temperature'])
      presKPA = float(record['pressure'])
      tempF = (tempC * 1.8) + 32
      presPSI = presKPA / 6.895

      record['temperature'] = tempF
      record['pressure'] = presPSI
      
      yield record
    except Exception:
      return


            
def run(argv=None):
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--input', required=True,
                      help='Input Pub/Sub topic')
  parser.add_argument('--output', required=True,
                      help='Output Pub/Sub topic')

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  
  with beam.Pipeline(options=pipeline_options) as p:
    cleaned = (
      p | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input)
      | "Clean and Convert" >> beam.ParDo(CleanMeasurementsDoFn())
      )
    (
      cleaned
      | "Convert to JSON" >> beam.Map(
        lambda x: json.dumps(x).encode('utf-8'))
      | "Write to PubSub" >> beam.io.WriteToPubSub(topic=known_args.output)
    
    )
        
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()