import settings
import tkinter as tk
from src.bridge import Bridge, MSG_TYPE



# SET WINDOW PARAMS
root = tk.Tk()
root.title("OSE SENDER")
root.geometry("300x200+300+300")

# MAIN CLASS
cpe_handler = Bridge()

# LABEL RESULT
lbl_result = tk.Message(root, text="                                  ", width=280)
lbl_result.grid(row=4, column=0)
def loading():
    lbl_result.configure(text="loading ...", fg="black")

# BUTTON DOWNLOAD
def download_handler():
    loading()
    result_msg = cpe_handler.call_download_cpe()
    color = 'black' if result_msg.type is MSG_TYPE.SUCCESS else 'red'
    lbl_result.configure(text=result_msg.content,
     fg=color)
btn_download = tk.Button(root, text="DOWNLOAD", command=download_handler, width=10)
btn_download.grid(row=0, column=0)

# BUTTON SEND
def send_handler():
    loading()
    result_msg = cpe_handler.call_send_cpe()
    lbl_result.configure(text=result_msg.content,
     fg='black' if result_msg.type is MSG_TYPE.SUCCESS else 'red')
btn_send = tk.Button(root, text="SEND", command=send_handler, width=10)
btn_send.grid(row=1, column=0)

# BUTTON CONFIRM
def confirm_handler():
    loading()
    result_msg = cpe_handler.call_confirm_cpe()
    lbl_result.configure(text=result_msg.content,
     fg='black' if result_msg.type is MSG_TYPE.SUCCESS else 'red')
btn_confirm = tk.Button(root, text="CONFIRM", command=confirm_handler, width=10)
btn_confirm.grid(row=2, column=0)

root.mainloop()
