from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime
import os, logging
import yaml

import pandas as pd

from dotenv import load_dotenv, find_dotenv
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from .base_etl import BaseETLProcessor

# generic email sending function
def send_email_alert(subject: str, html_content: str, sender_email: str, receiver_emails: List[str], password: str):
  msg = MIMEMultipart()
  msg['Subject'] = subject
  msg['From'] = sender_email
  msg['To'] = ", ".join(receiver_emails)

  body_html = MIMEText(html_content, 'html')
  msg.attach(body_html)

  smtp_server = "smtp.gmail.com"
  port = 587

  context = ssl.create_default_context()
  try:
    server = smtplib.SMTP(smtp_server, port)
    server.ehlo()
    server.starttls(context=context)
    server.ehlo()
    server.login(sender_email, password)

    server.sendmail(sender_email, receiver_emails, msg.as_string())
    print('Alert email sent')

  except Exception as e:
    print(f"Error sending email: {e}")
  finally:
    server.quit()

# Usage example:
# subject = f'Alert: System Status Report - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
# html_content = f"""
# <html>
#   <body>
#     <h2>System Status Alert</h2>
#     <p>Status: {'<span style="color:red">ERROR</span>' if error_condition else '<span style="color:green">OK</span>'}</p>
#     <p>Details: {error_details}</p>
#   </body>
# </html>
# """
# send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)


# Helper methods for use in the scripts that run the ETL processes

def load_config(config_path: Path) -> Dict[str, Any]:
  with config_path.open('r') as file:
    return yaml.safe_load(file)

def get_next_job_id(csv_path: Path, job_prefix: str) -> str:
  """
  Determines the next job ID based on the last entry in the historical runs CSV file.

  This function reads a CSV file specified by `csv_path`. If the file does not exist or is empty,
  it returns a default job ID "nearby_solds_1". Otherwise, it checks the last entry in the file:
  - If the last job's `all_status` is False, indicating a failure, it returns the same job ID to retry.
  - If the last job was successful, it increments the job number by 1 and returns the new job ID.

  Parameters:
  - csv_path (Path): The path to the CSV file containing job records.

  Returns:
  - str: The next job ID to be used.
  """
  if not csv_path.exists():
    return f"{job_prefix}_1"
  
  df = pd.read_csv(csv_path)
  
  if df.empty:
    return f"{job_prefix}_1"
  
  last_job = df.iloc[-1]
  last_job_id = last_job['job_id']
  last_number = int(last_job_id.split('_')[-1])
  
  if last_job['all_status'] == False:
    return last_job_id  # Rerun the failed job
  else:
    return f"{job_prefix}_{last_number + 1}"

def update_run_csv(csv_path: Path, job_id: str, processor: BaseETLProcessor) -> bool:
  """
  Updates a CSV file with the status of ETL job stages and sends an email alert if any stage fails.

  This function takes the path to a CSV file, a job ID, and an ETL processor object. It updates the CSV file with a new row containing the job ID, the status of each ETL stage (extract, transform, load, plus any additional stages defined in the processor), and a timestamp. If all stages are successful, it marks the overall status as successful. Otherwise, it sends an email alert about the failure.

  Parameters:
  - csv_path (Path): The file system path to the CSV file that tracks the run statuses.
  - job_id (str): The unique identifier for the ETL job.
  - processor (BaseETLProcessor): An instance of a class derived from BaseETLProcessor, which defines the ETL stages and their success status.

  Returns:
  - bool: True if all stages were successful, False otherwise.
  """
  standard_stages = ['extract', 'transform', 'load']

  # get all stages, incl. extra stages
  all_stages = standard_stages + processor.extra_stages

  # Check status for all stages
  # if all stages are successful, the individual stage marker files will get clean, 
  # so the individual stages are all assumed successful if 'all' is successful
  all_status = processor._was_success('all')

  stage_statuses = {
      stage: processor._was_success(stage) or all_status
      for stage in all_stages
  }

  # Create the new row data
  new_row_data = {
      'job_id': job_id,
      'all': all_status,
      'timestamp': datetime.now().isoformat()
  }
  new_row_data.update({stage: status for stage, status in stage_statuses.items()})

  new_row = pd.DataFrame([new_row_data])
  
  if csv_path.exists():
    df = pd.read_csv(csv_path)
    df = pd.concat([df, new_row], ignore_index=True)
  else:
    df = new_row
  
  df.to_csv(csv_path, index=False)

  # Send email alert if all_status is False
  if not all_status:
    try:
      send_etl_failure_alert(job_id, stage_statuses)
    except Exception as e:
      logging.error(f"Error occurred while attempting to send failure alert: {str(e)}")

  return all_status


def send_etl_failure_alert(job_id: str, stage_statuses: Dict[str, bool]):
    try:
      _ = load_dotenv(find_dotenv())

      sender_email = os.getenv('ANALYTICS_ETL_SENDER_EMAIL', None)
      receiver_emails = os.getenv('ANALYTICS_ETL_RECEIVER_EMAILS', '').split(',')
      email_password = os.getenv('ANALYTICS_ETL_EMAIL_PASSWORD', None)

      if sender_email is None or email_password is None or not receiver_emails:
        logging.warning("Email credentials or receivers not found in environment variables. Email alert will not be sent.")
        return

      subject = f"ETL Job Failure Alert: {job_id}"

      html_content = f"""
      <html>
        <body>
          <h2>ETL Job Failure Alert</h2>
          <p>Job ID: {job_id}</p>
          <p>Status: <span style="color:red">FAILED</span></p>
          <h3>Stage Statuses:</h3>
          <ul>
      """

      for stage, status in stage_statuses.items():
          color = "green" if status else "red"
          html_content += f"<li>{stage}: <span style='color:{color}'>{status}</span></li>"

      html_content += """
          </ul>
          <p>Please check the logs for more details.</p>
        </body>
      </html>
      """

      send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)
    except Exception as e:
      logging.error(f"Failed to send ETL email alert: {str(e)}")
