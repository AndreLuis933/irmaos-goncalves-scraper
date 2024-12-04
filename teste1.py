from math import prod
from time import sleep
import undetected_chromedriver as uc
from PoorMansHeadless import FakeHeadless
from a_cv_imwrite_imread_plus import open_image_in_cv
from cv2imshow.cv2imshow import cv2_imshow_single

def get_hwnd(driver):
    while True:
        try:
            allhwnds=[x for x in FakeHeadless.get_all_windows_with_handle() if x.pid == driver.browser_pid]
            return sorted(allhwnds, key=lambda x: prod(x.dim_win), reverse=True)[0].hwnd
        except Exception:
            continue
if __name__ == "__main__":
    driver = uc.Chrome()
    driver.get('http://www.google.com')
    driver.get_screenshot_as_png()
    sleep(2)
    hwnd=get_hwnd(driver)
    driverheadless=FakeHeadless(hwnd)
    driverheadless.start_headless_mode(width=None, height=None, distance_from_taskbar=1)
    screenshot1=lambda: cv2_imshow_single(open_image_in_cv(driver.get_screenshot_as_png()),title='sele1',killkeys="ctrl+alt+q")



