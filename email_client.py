import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class EmailClient(object):
    """
    Email Client to manage connection information for SMTP server
    """
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def _create_connection(self):
        server = smtplib.SMTP_SSL(self.host, self.port)
        server.ehlo()
        server.login(self.username, self.password)

        return server

    def send(self, from_address, to_address, subject, html_message, plain_message=None):
        server = self._create_connection()
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = from_address
        message['To'] = to_address

        if plain_message:
            message.attach(MIMEText(plain_message, 'plain'))
        message.attach(MIMEText(html_message, 'html'))

        server.sendmail(from_address, to_address, message.as_string())
        server.close()
