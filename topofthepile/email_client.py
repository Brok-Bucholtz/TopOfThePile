import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class EmailClient(object):
    """
    Email Client to manage connection information for SMTP server
    """
    def __init__(self, host, port, username, password, use_ssl=False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl

    def _create_connection(self):
        if self.use_ssl:
            server = smtplib.SMTP_SSL(self.host, self.port)
        else:
            server = smtplib.SMTP(self.host, self.port)
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

    def email_jobs(self, jobs, from_address, to_address, use_ssl):
        plural = 's' if len(jobs) > 1 else ''
        html_message = '<html>{}</html>'.format(
            '<br \>'.join(['<a href="{}">{}</a>'.format(job['url'], job['jobtitle']) for job in jobs]))
        self.send(
            from_address,
            to_address,
            'Top of the Pile: Found {} Job{}'.format(len(jobs), plural),
            html_message,
            use_ssl.lower() == 'true')
