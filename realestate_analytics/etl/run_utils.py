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
  
  This function always increments the job number by 1, regardless of the success/failure
  status of the previous job. The retry logic is now handled separately by checking
  if the previous job failed before starting a new run.

  Parameters:
  - csv_path (Path): The path to the CSV file containing job records.
  - job_prefix (str): The prefix for the job ID (e.g., "nearby_solds").

  Returns:
  - str: The next job ID to be used (always incremented).
  """
  if not csv_path.exists():
    return f"{job_prefix}_1"
  
  df = pd.read_csv(csv_path)
  
  if df.empty:
    return f"{job_prefix}_1"
  
  last_job = df.iloc[-1]
  last_job_id = last_job['job_id']
  last_number = int(last_job_id.split('_')[-1])
  
  # Always increment the job number
  return f"{job_prefix}_{last_number + 1}"

def is_last_run_successful(csv_path: Path) -> bool:
  """
  Check if the last ETL run was successful by examining the CSV file.
  
  This function reads the CSV file and checks the 'all' status of the most recent entry.
  If the file doesn't exist or is empty, it's considered successful (first run scenario).

  Parameters:
  - csv_path (Path): The path to the CSV file containing job records.

  Returns:
  - bool: True if the last run was successful or if no previous runs exist, False otherwise.
  """
  if not csv_path.exists():
    return True  # No previous runs, consider as successful
  
  df = pd.read_csv(csv_path)
  
  if df.empty:
    return True  # No previous runs, consider as successful
  
  last_job = df.iloc[-1]
  return bool(last_job['all'])  # Convert to bool in case it's stored as string


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
  # TODO: Test this only on PROD env, uncomment before deployment
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


def send_zero_sold_listings_alert(job_id: str, prov_code: str, start_time, end_time):
  """
  Send email alert when zero sold listings are found (highly unexpected scenario).

  This is a warning alert (not a failure) - the ETL continues successfully but
  devops is notified to investigate potential data issues.

  Args:
      job_id: The ETL job identifier
      prov_code: Province code (e.g., 'ON', 'BC')
      start_time: Start of the query time range
      end_time: End of the query time range
  """
  try:
    _ = load_dotenv(find_dotenv())
    sender_email = os.getenv('ANALYTICS_ETL_SENDER_EMAIL')
    receiver_emails = os.getenv('ANALYTICS_ETL_RECEIVER_EMAILS', '').split(',')
    email_password = os.getenv('ANALYTICS_ETL_EMAIL_PASSWORD')

    if not sender_email or not email_password or not receiver_emails:
      logging.warning("Email credentials not configured. Zero sold listings alert not sent.")
      return

    subject = f"⚠️ URGENT: Zero Sold Listings Detected - {job_id}"
    html_content = f"""
    <html>
      <body>
        <h2 style="color:orange">⚠️ Zero Sold Listings Alert</h2>
        <p><strong>Job ID:</strong> {job_id}</p>
        <p><strong>Province:</strong> {prov_code}</p>
        <p><strong>Time Range:</strong> {start_time} to {end_time}</p>
        <p><strong>Status:</strong> <span style="color:orange">WARNING</span> (ETL continues, but investigation needed)</p>
        <p>No sold listings were found in the specified time range. This is highly unexpected and should be investigated immediately.</p>
        <h3>Possible Causes:</h3>
        <ul>
          <li>Data ingestion pipeline failure</li>
          <li>Elasticsearch index issues</li>
          <li>Query filter problems</li>
          <li>Legitimately quiet period (rare)</li>
        </ul>
        <p><strong>Action Required:</strong> Check ES index health and data ingestion status ASAP.</p>
      </body>
    </html>
    """

    send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)
    logging.info("Zero sold listings alert email sent successfully")
  except Exception as e:
    logging.error(f"Failed to send zero sold listings alert: {e}")


def send_absorption_rate_fetch_failure_alert(job_id: str, prov_code: str, error: str):
  """
  Send email alert when AbsorptionRateProcessor's live active-listing-count fetch (ES query)
  fails with a genuine error (connection/request/unexpected) for a province in
  ASSUME_ZERO_ON_FETCH_FAILURE (currently NT, NU). get_current_active_listings() now returns
  success=True with an empty DataFrame for a legitimate zero-hit query, so a success=False
  here means a real failure, not a zero.

  Only called for the tolerated case: for those provinces the failure is swallowed (assumed
  zero) and no exception is raised, so this is the *only* place this specific failure can ever
  be surfaced - there's no exception for a wrapping try/except to catch and alert on elsewhere.
  For every other province, _fetch_active_listing_counts() raises instead of calling this, and
  transform()'s own wrapper around _handle_month_end_snapshot() sends the one alert for that
  case (send_absorption_rate_snapshot_error_alert) - not this function, to avoid sending two
  emails for one failure.

  Args:
      job_id: The ETL job identifier
      prov_code: Province code (e.g., 'NT', 'NU')
      error: Description of the failure (e.g. the error message logged at the call site)
  """
  try:
    _ = load_dotenv(find_dotenv())
    sender_email = os.getenv('ANALYTICS_ETL_SENDER_EMAIL')
    receiver_emails = os.getenv('ANALYTICS_ETL_RECEIVER_EMAILS', '').split(',')
    email_password = os.getenv('ANALYTICS_ETL_EMAIL_PASSWORD')

    if not sender_email or not email_password or not receiver_emails:
      logging.warning("Email credentials not configured. Absorption rate fetch failure alert not sent.")
      return

    subject = f"⚠️ Absorption Rate: Live Listing Fetch Failed for {prov_code} (tolerated) - {job_id}"
    html_content = f"""
    <html>
      <body>
        <h2 style="color:orange">⚠️ Absorption Rate Active-Listing Fetch Failure (Tolerated)</h2>
        <p><strong>Job ID:</strong> {job_id}</p>
        <p><strong>Province:</strong> {prov_code}</p>
        <p><strong>Error:</strong> {error}</p>
        <p>The live Elasticsearch query for currently-active listings failed while handling
        the month-end snapshot for {prov_code}. This is most likely a transient ES connectivity
        issue. {prov_code} is in ASSUME_ZERO_ON_FETCH_FAILURE, so this is being tolerated
        (treated as zero active listings) and the ETL run continues normally - it does not
        fail or halt.</p>
        <p><strong>Action Required:</strong> Check ES cluster health/connectivity. Since the
        run continues, there is no urgency to unblock anything - but a genuine zero for
        {prov_code} this month should be verified against this alert, not assumed correct.</p>
      </body>
    </html>
    """

    send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)
    logging.info("Absorption rate fetch failure alert email sent successfully")
  except Exception as e:
    logging.error(f"Failed to send absorption rate fetch failure alert: {e}")


def send_etl_error_alert(job_id: str, context: str, error: str):
  """
  General-purpose email alert for an ETL failure that a processor wants to surface
  immediately, rather than waiting on update_run_csv()'s generic send_etl_failure_alert
  (which only reports stage pass/fail booleans, not the actual error). The caller supplies a
  short, freeform `context` string identifying what failed (e.g. "LastMthMetricsProcessor/ON
  end_of_mth_run") and the error detail - everything needed goes in these two strings rather
  than requiring a dedicated alert function per failure type.

  Intended usage: let the underlying exception propagate (don't swallow it) - call this from
  a single try/except wrapping the risky steps, log + send this alert, then re-raise so the
  existing halt-gate (is_last_run_successful()/run CSV) still applies, same as any other ETL
  failure in this codebase.

  Args:
      job_id: The ETL job identifier
      context: Short description of what was running when this failed (processor, province, stage)
      error: Description of the failure (e.g. str(exception))
  """
  try:
    _ = load_dotenv(find_dotenv())
    sender_email = os.getenv('ANALYTICS_ETL_SENDER_EMAIL')
    receiver_emails = os.getenv('ANALYTICS_ETL_RECEIVER_EMAILS', '').split(',')
    email_password = os.getenv('ANALYTICS_ETL_EMAIL_PASSWORD')

    if not sender_email or not email_password or not receiver_emails:
      logging.warning("Email credentials not configured. ETL error alert not sent.")
      return

    subject = f"⚠️ ETL Error: {context} - {job_id}"
    html_content = f"""
    <html>
      <body>
        <h2 style="color:red">⚠️ ETL Error</h2>
        <p><strong>Job ID:</strong> {job_id}</p>
        <p><strong>Context:</strong> {context}</p>
        <p><strong>Error:</strong> {error}</p>
        <p>Check the logs for the full traceback around this job ID.</p>
      </body>
    </html>
    """

    send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)
    logging.info("ETL error alert email sent successfully")
  except Exception as e:
    logging.error(f"Failed to send ETL error alert: {e}")


def send_absorption_rate_snapshot_push_failure_alert(job_id: str, prov_code: str, expected_yearmonth: str,
                                                       success: int, failed: int):
  """
  Send email alert when AbsorptionRateProcessor's month-end snapshot ES push
  (update_month_end_snapshot_in_es) fails below LOAD_SUCCESS_THRESHOLD.

  This is distinct from send_absorption_rate_fetch_failure_alert: the live ES fetch and local
  caching of the snapshot (current_counts_ts_df) already succeeded - only the push of that
  already-captured data to rlp_mkt_trends_current failed. Month-end snapshot handling only
  ever runs on the 1st of the month (no self-heal/retry window) - if this fires, devops needs
  to manually rerun the job to retry the push (safe to do: the snapshot is a time-blind live
  query and the local cache append is idempotent, so a rerun is a plain, no-state-to-reconcile
  retry).

  Args:
      job_id: The ETL job identifier
      prov_code: Province code (e.g., 'ON', 'BC')
      expected_yearmonth: The year-month (e.g., '2026-02') whose snapshot push failed
      success: Number of documents successfully updated
      failed: Number of documents that failed to update
  """
  try:
    _ = load_dotenv(find_dotenv())
    sender_email = os.getenv('ANALYTICS_ETL_SENDER_EMAIL')
    receiver_emails = os.getenv('ANALYTICS_ETL_RECEIVER_EMAILS', '').split(',')
    email_password = os.getenv('ANALYTICS_ETL_EMAIL_PASSWORD')

    if not sender_email or not email_password or not receiver_emails:
      logging.warning("Email credentials not configured. Absorption rate snapshot push failure alert not sent.")
      return

    subject = f"⚠️ Absorption Rate: Snapshot Push Failed for {prov_code}/{expected_yearmonth} - {job_id}"
    html_content = f"""
    <html>
      <body>
        <h2 style="color:red">⚠️ Month-End Snapshot ES Push Failure</h2>
        <p><strong>Job ID:</strong> {job_id}</p>
        <p><strong>Province:</strong> {prov_code}</p>
        <p><strong>Month:</strong> {expected_yearmonth}</p>
        <p><strong>Success/Failed:</strong> {success} / {failed}</p>
        <p>The month-end snapshot for {prov_code}/{expected_yearmonth} was already captured and
        cached locally, but pushing it to rlp_mkt_trends_current (mth_end_snapshot_listing_count)
        failed below the success threshold. There is no automatic retry - month-end snapshot
        handling only runs on the 1st of the month.</p>
        <p><strong>Action Required:</strong> Check ES cluster health/connectivity, then manually
        rerun the job. No data has been lost - the captured snapshot remains in the local cache
        regardless of the push outcome, and a rerun is a plain, idempotent retry.</p>
      </body>
    </html>
    """

    send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)
    logging.info("Absorption rate snapshot push failure alert email sent successfully")
  except Exception as e:
    logging.error(f"Failed to send absorption rate snapshot push failure alert: {e}")


def send_absorption_rate_snapshot_error_alert(job_id: str, prov_code: str, expected_yearmonth: str, error: str):
  """
  Send email alert when AbsorptionRateProcessor's _handle_month_end_snapshot() raises for any
  reason not already covered by a more specific alert. This is the catch-all - including a
  non-tolerated fetch failure (ASSUME_ZERO_ON_FETCH_FAILURE provinces alert internally via
  send_absorption_rate_fetch_failure_alert instead, since for them nothing raises and this
  wrapper never runs), local capture/caching, and building the ES payload from the
  already-cached snapshot. Push failures alert separately via
  send_absorption_rate_snapshot_push_failure_alert (that failure surfaces in load(), not here).

  Deliberately never re-raised at the call site: the daily absorption rate / months-of-
  inventory computation is the important, recurring work in transform(), and a bug isolated to
  the month-end snapshot side-quest must not be able to block it or halt the day's ETL run.

  Args:
      job_id: The ETL job identifier
      prov_code: Province code (e.g., 'ON', 'BC')
      expected_yearmonth: The year-month (e.g., '2026-02') being processed when this failed
      error: Description of the failure (e.g. the error message logged at the call site)
  """
  try:
    _ = load_dotenv(find_dotenv())
    sender_email = os.getenv('ANALYTICS_ETL_SENDER_EMAIL')
    receiver_emails = os.getenv('ANALYTICS_ETL_RECEIVER_EMAILS', '').split(',')
    email_password = os.getenv('ANALYTICS_ETL_EMAIL_PASSWORD')

    if not sender_email or not email_password or not receiver_emails:
      logging.warning("Email credentials not configured. Absorption rate snapshot error alert not sent.")
      return

    subject = f"⚠️ Absorption Rate: Snapshot Handling Error for {prov_code}/{expected_yearmonth} - {job_id}"
    html_content = f"""
    <html>
      <body>
        <h2 style="color:red">⚠️ Month-End Snapshot Handling Error</h2>
        <p><strong>Job ID:</strong> {job_id}</p>
        <p><strong>Province:</strong> {prov_code}</p>
        <p><strong>Month:</strong> {expected_yearmonth}</p>
        <p><strong>Error:</strong> {error}</p>
        <p>_handle_month_end_snapshot() raised while processing {prov_code}/{expected_yearmonth}.
        This has been swallowed - it does NOT block or fail the daily absorption rate ETL run,
        which is the important, recurring work. Month-end snapshot handling only runs on the
        1st of the month, so there is no automatic retry.</p>
        <p><strong>Action Required:</strong> Check the logs for the full traceback around this
        job ID to identify where in capture/caching/payload-building this failed, then manually
        rerun the job once resolved.</p>
      </body>
    </html>
    """

    send_email_alert(subject, html_content, sender_email, receiver_emails, email_password)
    logging.info("Absorption rate snapshot error alert email sent successfully")
  except Exception as e:
    logging.error(f"Failed to send absorption rate snapshot error alert: {e}")
