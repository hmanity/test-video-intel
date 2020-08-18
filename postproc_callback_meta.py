import json
import logging
import sys
import time
from datetime import datetime
import gi
import ciso8601

gi.require_version("Gst", "1.0")
import cv2
from gi.repository import GObject, Gst, GstVideo

from gstgva import RegionOfInterest, VideoFrame


Gst.init(sys.argv)
font = cv2.FONT_HERSHEY_SIMPLEX


class Processor(object):
    def __init__(self, argument=None):
        self.ts = argument
        self.fs = 0

    def process_frame(self, frame: VideoFrame) -> bool:

        msgs = frame.messages()

        for m in msgs:
            data = json.loads(m)
            if self.ts is not None:
                ts = int(ciso8601.parse_datetime(self.ts).timestamp())
                data["ts"] = int(ts) + int(data["timestamp"] / 1000000000)
            else:
                data["ts"] = int(time.time())

            self.fs = data["ts"]
            frame.remove_message(m)
            frame.add_message(json.dumps(data))
            # print(data)
        with frame.data() as mat:
            cv2.putText(mat, str(self.fs), (20, 20), font, 0.5, (255, 255, 255), 2)
        return True
