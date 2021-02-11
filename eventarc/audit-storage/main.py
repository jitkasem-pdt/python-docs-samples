# Copyright 2020 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START eventarc_gcs_server]
import os
from flask import Flask, request
from google.cloud import bigquery

app = Flask(__name__)
# [END eventarc_gcs_server]

# [START eventarc_gcs_handler]
@app.route('/', methods=['POST'])
def index():
    # Gets the GCS bucket name from the CloudEvent header
    # Example: "storage.googleapis.com/projects/_/buckets/my-bucket"
    # storage.googleapis.com/projects/_/buckets/test_dong/objects/support_site.csv
    bucket = request.headers.get('ce-subject')
    tmp_bucket = bucket.split("/")
    ttt = tmp_bucket[4]
    qqq = tmp_bucket[6]
    
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "thailife.cloud_run.test_tbl"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("post_abbr", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    
    uri = f"gs://{ttt}/{qqq}"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  
    # Make an API request.
    # print("Loaded {} rows.".format(destination_table.num_rows))

    print(f"Detected change in GCS bucket: {bucket}")
    return (f"Detected change in GCS bucket: {bucket}", 200)
# [END eventarc_gcs_handler]


# [START eventarc_gcs_server]
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
# [END eventarc_gcs_server]
