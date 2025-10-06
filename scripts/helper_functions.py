import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# --- Configuration ---
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
SENDER_EMAIL = 'tindangbao5603@gmail.com'
SENDER_PASSWORD = 'imboqodkokyeyjhj'

def send_email(recipient_email, subject, body):
    """A generic function to send an email using SMTP."""
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("ERROR: SENDER_EMAIL or SENDER_PASSWORD environment variables not set.")
        print("Email not sent.")
        return False

    try:
        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html')) # Use 'html' for rich text formatting

        # Connect to the SMTP server and send the email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Secure the connection
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
            print(f"Successfully sent email to {recipient_email}")
            return True
    except Exception as e:
        print(f"Failed to send email to {recipient_email}. Error: {e}")
        return False

def send_delayed_payment_alert(customer_name, customer_email, order_id, order_date, order_amount, currency="USD"):
    """Sends a payment reminder to a customer."""
    subject = f"Action Required: Your Payment for Order #{order_id} is Overdue"
    
    # Using an HTML template for better formatting
    body = f"""
    <html>
    <body>
        <p>Hi <strong>{customer_name}</strong>,</p>
        <p>We're writing to remind you that the payment for your recent order is now overdue.</p>
        <p>Here are the details of the transaction:</p>
        <ul>
            <li><strong>Order ID:</strong> {order_id}</li>
            <li><strong>Order Date:</strong> {order_date}</li>
            <li><strong>Total Amount:</strong> {order_amount} {currency}</li>
        </ul>
        <p>To avoid any disruption or cancellation of your order, please complete your payment as soon as possible. You can make the payment by clicking the secure link below:</p>
        <p><a href="https://yourcompany.com/pay/{order_id}" style="background-color: #007bff; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px;">Pay Now</a></p>
        <p>If you've already made the payment, please disregard this email. If you're facing any issues or have questions about your order, please don't hesitate to contact our support team.</p>
        <p>Thank you,<br>The COMS Team</p>
    </body>
    </html>
    """
    send_email(customer_email, subject, body)

def send_etl_error_alert(tech_email, pipeline_stage, table_name, error_message):
    """Sends a critical failure alert to the tech team."""
    subject = f"CRITICAL: ETL Job Failed - {pipeline_stage} - {table_name}"
    
    body = f"""
    <html>
    <body>
        <h2>ETL Process Alert: FAILURE</h2>
        <p>An error occurred during the data processing pipeline. Immediate investigation is required.</p>
        <h3>Details:</h3>
        <ul>
            <li><strong>Timestamp (UTC):</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}</li>
            <li><strong>Pipeline Stage:</strong> {pipeline_stage}</li>
            <li><strong>Table/Process:</strong> {table_name}</li>
        </ul>
        <h3>Error Message:</h3>
        <pre style="background-color: #f8d7da; color: #721c24; padding: 10px; border: 1px solid #f5c6cb; border-radius: 5px;">{error_message}</pre>
    </body>
    </html>
    """
    send_email(tech_email, subject, body)

def send_etl_success_notification(tech_email, pipeline_stage, output_path):
    """Sends a success confirmation to the tech team."""
    subject = f"SUCCESS: ETL Job Completed - {pipeline_stage}"
    
    body = f"""
    <html>
    <body>
        <h2>ETL Process Alert: SUCCESS</h2>
        <p>The following process - <strong>{pipeline_stage}</strong> - has been processed and loaded successfully.</p>
        <h3>Details:</h3>
        <ul>
            <li><strong>Timestamp (UTC):</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}</li>
            <li><strong>Output Path:</strong> {output_path}</li>
        </ul>
        <p>No action is required. This is an automated success notification.</p>
    </body>
    </html>
    """
    send_email(tech_email, subject, body)