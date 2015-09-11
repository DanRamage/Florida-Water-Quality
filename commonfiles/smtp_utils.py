#Class borrowed from here: http://www.pastequestion.com/blog/python/send-email-with-attachments-using-python.html

import smtplib, os, time, atexit
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
 
class ConnectionError(smtplib.SMTPException): pass
class LoginError(smtplib.SMTPException): pass
class DisconnectionError(smtplib.SMTPException): pass
class EmailSendError(smtplib.SMTPException): pass
 
 

class smtpClass:
 
  def __init__(self, host, user, password, port=25, use_tls=False):
    self._host        = host
    self._port        = port                
    self._user        = user
    self._password    = password
    self._use_tls     = use_tls
    self._message     = None
    self._subject     = None
    self._from_addr   = None
    self._rcpt_to     = None               
    self._server      = None         
    self._attachments = []
  
    atexit.register(self.close) #our close() method will be automatically executed upon normal interpreter termination
  
    self.connect()
  
  
  def connect(self):
  
    if all([self._host, self._port, self._user, self._password]):  
      try:
        self._server = smtplib.SMTP(self._host, self._port)
        if self._use_tls:
          self._server.starttls()
      except smtplib.SMTPException, e:
        raise ConnectionError("Connection failed!")    
      try:
        self._server.login(self._user, self._password)            
      except smtplib.SMTPException, e:                                
        raise LoginError("Login Failed!")
  
  
  
  def close(self):                  
    if self._server:
      try:
        self._server.quit()    
      except smtplib.SMTPException, e:                                
        raise DisconnectionError("Disconnection failed!")
  
  
  def message(self, message):
    self._message = message
  
  
  def subject(self, subject):
    self._subject = subject
  
  
  def from_addr(self, email):
    self._from_addr = email
  
  
  def rcpt_to(self, email):
    self._rcpt_to = email
  
  
  def attach(self, file):
    if os.path.exists(file):
            self._attachments.append(file)
  
  
  def load_attachments(self, m_message):
    for file in self._attachments:
      part = MIMEBase('application', "octet-stream")
      part.set_payload(open(file,"rb").read())
      Encoders.encode_base64(part)
      part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(file))
      m_message.attach(part)    
  
    return m_message
  
  
  def send(self, content_type='plain', charset='UTF-8'):
  
    if all([self._message, self._subject, self._from_addr, self._rcpt_to]):                                  
      m_message             = MIMEMultipart()
  
      m_message['From']     = self._from_addr
      m_message['To']       = COMMASPACE.join(self._rcpt_to)
      m_message['Date']     = formatdate(localtime=True)
      m_message['Subject']  = self._subject
      m_message['X-Mailer'] = "Python X-Mailer"
  
      m_message.attach(MIMEText(self._message, content_type, charset))
  
      m_message = self.load_attachments(m_message)
  
      try:
              self._server.sendmail(self._from_addr, self._rcpt_to, m_message.as_string())       
  
      except smtplib.SMTPException, e:
              raise EmailSendError("Email has not been sent")