from airflow.utils.email import send_email

def send_email_callback(context):
    """
    Send a custom email on task failure.
    """
    task_instance = context['task_instance']

    subject = f"Airflow Alert: Task Failed - {task_instance.task_id}"
    html_content = f"""
    <h3>Task Failure Alert</h3>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>DAG:</strong> {task_instance.dag_id}</p>
    <p><strong>Start date:</strong> {task_instance.start_date }</p>
    """
    send_email(
        to=['your_email@localhost'],  # MailHog will catch all emails sent to this domain
        subject=subject,
        html_content=html_content
    )
