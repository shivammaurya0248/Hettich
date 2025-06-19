import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os


def send_email(file_path=None):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587  # STARTTLS

    sender_email = "Aristiotsolutions@gmail.com"
    sender_password = "wmajnrtwyctvxxyt"  # App password
    # receiver_email = "arvind@aristautomation.com"
    receiver_email = "harpreesidhu1997@gmail.com"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = "SMTP Test Email"

    body = "This is a test email to verify SMTP credentials."
    msg.attach(MIMEText(body, 'plain'))

    if file_path and os.path.isfile(file_path):
        filename = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            part = MIMEApplication(f.read(), _subtype=os.path.splitext(filename)[1][1:])
            part.add_header('Content-Disposition', 'attachment', filename=filename)
            msg.attach(part)
        print(f"📎 Attached file: {filename}")
    elif file_path:
        print(f"⚠️ File not found at {file_path}")

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print("✅ Email sent successfully via STARTTLS!")
    except Exception as e:
        print(f"❌ Error sending email: {e}")


# Test
send_email()  # No attachment
# send_email("C:/path/to/file.txt")  # With attachment
