#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from .request import DBRoutine, Request
from PySide.QtCore import *
from PySide.QtGui import *
from PySide.QtWebKit import *
from PySide.QtNetwork import *


class HTSQLNetworkReply(QNetworkReply):

    def __init__(self, parent, request, operation):
        super(HTSQLNetworkReply, self).__init__(parent)
        self.setRequest(request)
        self.setUrl(request.url())
        self.setOperation(operation)
        url = request.url()
        if url.scheme() != 'http' or url.authority() != 'htsql':
            self.body = ""
            self.offset = 0
            self.setError(self.ProtocolUnknownError, "unsupported protocol")
            QTimer.singleShot(0,
                    lambda: self.error.emit(self.ProtocolUnknownError))
            QTimer.singleShot(0, lambda: self.finished.emit())
            return
        if operation != parent.GetOperation:
            self.body = ""
            self.offset = 0
            self.setError(self.ProtocolInvalidOperationError,
                          "unsupported protocol operation")
            QTimer.singleShot(0,
                    lambda: self.error(self.ProtocolInvalidOperationError))
            QTimer.singleShot(0, lambda: self.finished.emit())
            return
        query = str(url.toString(url.RemoveScheme|url.RemoveAuthority))
        extra_headers = {}
        extra_headers['Host'] = str(url.host())
        for header in request.rawHeaderList():
            extra_headers[str(header)] = str(request.rawHeader(header))
        response = Request.prepare('GET', query, extra_headers=extra_headers) \
                        .execute(parent.app)
        code, phrase = response.status.split(' ', 1)
        self.setAttribute(request.HttpStatusCodeAttribute, int(code))
        self.setAttribute(request.HttpReasonPhraseAttribute, phrase)
        for header, value in response.headers:
            self.setRawHeader(header, value)
        self.body = response.body
        self.offset = 0
        self.operation = operation
        self.open(self.ReadOnly | self.Unbuffered)
        QTimer.singleShot(0, lambda: self.readyRead.emit())
        QTimer.singleShot(0, lambda: self.finished.emit())

    def abort(self):
        pass

    def bytesAvailable(self):
        return (len(self.body)-self.offset +
                super(HTSQLNetworkReply, self).bytesAvailable())

    def isSequential(self):
        return True

    def readData(self, maxSize):
        size = min(len(self.body)-self.offset, maxSize)
        data = self.body[self.offset:self.offset+size]
        self.offset += size
        if data:
            return data


class HTSQLNetworkAccessManager(QNetworkAccessManager):

    def __init__(self, app):
        super(HTSQLNetworkAccessManager, self).__init__()
        self.app = app

    def createRequest(self, operation, request, data):
        return HTSQLNetworkReply(self, request, operation)


class HTSQLForm(QDialog):

    def __init__(self, app, parent=None):
        super(HTSQLForm, self).__init__(parent)
        self.setWindowTitle(app.htsql.db.database)
        self.app = app
        self.manager = HTSQLNetworkAccessManager(app)
        self.edit = QLineEdit("/")
        self.view = QWebView()
        self.view.page().setNetworkAccessManager(self.manager)
        layout = QVBoxLayout()
        layout.addWidget(self.edit)
        layout.addWidget(self.view)
        self.setLayout(layout)
        self.edit.returnPressed.connect(self.query)
        self.edit.returnPressed.emit()

    def query(self):
        uri = self.edit.text().strip()
        if not uri.startswith("/"):
            return
        uri = "http://htsql"+uri
        self.view.load(QUrl(uri))


class UIRoutine(DBRoutine):

    name = 'ui'

    def start(self, app):
        qt = QApplication([self.executable])
        form = HTSQLForm(app)
        form.show()
        return qt.exec_()


