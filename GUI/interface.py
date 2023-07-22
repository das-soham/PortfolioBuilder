from GUI.MainWindow import Ui_MainWindow
from PySide6.QtWidgets import QApplication, QMainWindow
import random


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setupUi(self)   #not needed when we are using .uic file
        self.show()
        self.pushButton_1.clicked.connect(self.update_title)

    def update_title(self):
        n = random.randint(1, 10)
        self.label.setText("%d" % n)

app = QApplication([])
w = MainWindow()
app.exec()
