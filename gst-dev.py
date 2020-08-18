import json
import logging
import sys
import time
from argparse import ArgumentParser

import ciso8601
import cv2
import gi

try:
    #    gi.require_version('GstApp', '1.0')
    #    gi.require_version('GstVideo', '1.0')
    #    gi.require_version('GObject', '2.0')
    gi.require_version('Gst', '1.0')
    gi.require_version('GstRtspServer', '1.0')
    from gi.repository import GLib, Gst, GstRtspServer
    from gstgva import VideoFrame, util

except ValueError as exc:
    print(exc)
    sys.exit()

parser = ArgumentParser(add_help=False)
_args = parser.add_argument_group('Options')
_args.add_argument("-i", "--input", help="Required. Path to input rtsp feed uri",
                   required=True, type=str)
_args.add_argument("-u", "--user", help="Required. Username rtsp feed",
                   required=True, type=str)
_args.add_argument("-p", "--password", help="Required. Password rtsp feed",
                   required=True, type=str)
_args.add_argument("-a", "--archive", required=False, type=bool, default=False)
_args.add_argument("-s", "--sink", required=False, type=str, default='fake')

args = parser.parse_args()

start_ts = None
if start_ts is not None:
    start_ts = int(ciso8601.parse_datetime(start_ts).timestamp())

tags = {"ap": "AP29",
        "computer": "AP29kassa1"}

src = args.input

jtags = json.dumps(tags)
if args.archive:
    topic = f'{src}-{year}-{month}-{day}'
else:
    topic = f'{src}-online'

updsink_port_num = 5400
codec = "H264"
USE_TIMEOVERLAY = False

v_crop = "top=10 left=400 right=200 bottom=200"
detection_model = 'face-detection-retail-0004'
landmarks_model = 'landmarks-regression-retail-0009'
identification_model = 'face-reidentification-retail-0095'
head_pose_model = 'head-pose-estimation-adas-0001'


def get_model_path(model):
    return f'./models/intel/{model}/FP32-INT8/{model}.xml'


def get_model_proc(model):
    return f'./model_proc/{model}.json'


def draw_conf(frame: VideoFrame):
    with frame.data() as img:
        for roi in frame.regions():
            rect = roi.rect()
            conf = roi.confidence()
            if rect:
                cv2.putText(img, f'{conf:.2f}',
                            (rect.x, rect.y + rect.h + 30),
                            cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)


def draw_ts(frame: VideoFrame) -> bool:
    font = cv2.FONT_HERSHEY_SIMPLEX
    msgs = frame.messages()
    for m in msgs:
        data = json.loads(m)
        if start_ts is not None:
            data["ts"] = int(start_ts) + int(data["timestamp"] / 1000000000)
        else:
            data["ts"] = int(time.time())
        frame_ts = data["ts"]
        frame.remove_message(m)
        frame.add_message(json.dumps(data))
    with frame.data() as mat:
        cv2.putText(mat, str(frame_ts), (20, 20), font, 1, (255, 255, 255), 2)
    return True


def pad_wm_probe_callback(pad, info):
    with util.GST_PAD_PROBE_INFO_BUFFER(info) as buffer:
        caps = pad.get_current_caps()
        frame = VideoFrame(buffer, caps=caps)
        draw_conf(frame)
    return Gst.PadProbeReturn.OK


def info_probe_callback(pad, info):
    info_event = info.get_event()
    info_structure = info_event.get_structure()
    print(info_structure)
    return Gst.PadProbeReturn.PASS


def pad_mc_probe_callback(pad, info):
    with util.GST_PAD_PROBE_INFO_BUFFER(info) as buffer:
        caps = pad.get_current_caps()
        frame = VideoFrame(buffer, caps=caps)
        draw_ts(frame)
    return Gst.PadProbeReturn.OK


def init_pipe():
    Gst.init(sys.argv)
    sink = args.sink
    THRESHOLD = 0.65
    crop = {'top': 0,
            'right': 0,
            'left': 0,
            'bottom': 0}
    caps = Gst.caps_from_string("video/x-raw,format=BGRx")

    kafka_server = '10.42.0.26:9092'
    kafka_topic = 'test'

    logging.info("Creating Pipeline")
    pipeline = Gst.Pipeline()
    if not pipeline:
        logging.error("Unable to create Pipeline")

    logging.info("Creating Source")
    source = Gst.ElementFactory.make("rtspsrc", "rtsp-source")
    if not source:
        logging.error("Unable to create Source")
    source.set_property('location', args.input)
    source.set_property('user-id', args.user)
    source.set_property('user-pw', args.password)
    source.set_property('protocols', 'tcp')
    source.set_property('latency', 0)
    source.set_property('do-rtcp', 'yes')
#    source.set_property('ntp-sync', True)
#    source.set_property('ntp-time-source', 'ntp')

    logging.info("Creating decoder")
    decoder = Gst.ElementFactory.make("decodebin", "decoder")
    if not decoder:
        logging.error("Unable to create decoder")

    logging.info("Creating videocrop")
    videocrop = Gst.ElementFactory.make("videocrop", "videocrop")
    if not videocrop:
        logging.error("Unable to create videocrop")
    videocrop.set_property('top', crop['top'])
    videocrop.set_property('left', crop['left'])
    videocrop.set_property('right', crop['right'])
    videocrop.set_property('bottom', crop['bottom'])

    queue1 = Gst.ElementFactory.make("queue", "queue1")
    if not queue1:
        logging.error("Unable to create queue1")
    queue2 = Gst.ElementFactory.make("queue", "queue2")
    if not queue2:
        logging.error("Unable to create queue2")
    queue3 = Gst.ElementFactory.make("queue", "queue3")
    if not queue3:
        logging.error("Unable to create queue3")
    queue4 = Gst.ElementFactory.make("queue", "queue4")
    if not queue4:
        logging.error("Unable to create queue4")
    queue5 = Gst.ElementFactory.make("queue", "queue5")
    if not queue5:
        logging.error("Unable to create queue5")

    converter1 = Gst.ElementFactory.make("videoconvert", "videoconverter1")
    if not converter1:
        logging.error("Unable to create videoconverter1")
    converter1.set_property("n-threads", 4)
    converter2 = Gst.ElementFactory.make("videoconvert", "videoconverter2")
    if not converter2:
        logging.error("Unable to create videoconverter2")
    converter2.set_property("n-threads", 4)

    capsfilter = Gst.ElementFactory.make("capsfilter", "capsfilter")
    if not capsfilter:
        logging.error("Unable to create capsfilter")
    capsfilter.set_property('caps', caps)

    face_detector = Gst.ElementFactory.make("gvadetect", "gvadetect")
    if not face_detector:
        logging.error("Unable to create face detector")
    face_detector.set_property("threshold", THRESHOLD)
    face_detector.set_property("model", get_model_path(detection_model))
    face_detector.set_property("model-proc", get_model_proc(detection_model))
    face_detector.set_property("pre-process-backend", "opencv")

    gvaclassify1 = Gst.ElementFactory.make("gvaclassify", "gvaclassify1")
    if not gvaclassify1:
        logging.error("Unable to create gvaclassify1")
    gvaclassify1.set_property("model", get_model_path(landmarks_model))
    gvaclassify1.set_property("model-proc", get_model_proc(landmarks_model))
    gvaclassify1.set_property("pre-process-backend", "opencv")

    gvaclassify2 = Gst.ElementFactory.make("gvaclassify", "gvaclassify2")
    if not gvaclassify2:
        logging.error("Unable to create gvaclassify2")
    gvaclassify2.set_property("model", get_model_path(identification_model))
    gvaclassify2.set_property(
        "model-proc", get_model_proc(identification_model))
    gvaclassify2.set_property("pre-process-backend", "opencv")

    gvametaconvert = Gst.ElementFactory.make(
        "gvametaconvert", "gvametaconvert")
    if not gvametaconvert:
        logging.error("Unable to create gvametaconvert")
    gvametaconvert.set_property("add-empty-results", 'true')
    gvametaconvert.set_property("format", 'json')
    gvametaconvert.set_property("add-tensor-data", "true")
    gvametaconvert.set_property("source", 'test')
    gvametaconvert.set_property("tags", jtags)

    gvametapublish = Gst.ElementFactory.make(
        "gvametapublish", "gvametapublish")
    if not gvametapublish:
        logging.error("Unable to create gvametapublish")
    gvametapublish.set_property("method", 'kafka')
    gvametapublish.set_property("address", kafka_server)
    gvametapublish.set_property("topic", kafka_topic)

    logging.info("Adding elements to Pipeline")
    pipeline.add(source)
    pipeline.add(decoder)
    pipeline.add(videocrop)
    pipeline.add(queue1)
    pipeline.add(converter1)
    pipeline.add(capsfilter)
    pipeline.add(face_detector)
    pipeline.add(queue2)
    pipeline.add(gvaclassify1)
    pipeline.add(queue3)
    pipeline.add(gvaclassify2)
    pipeline.add(queue4)
    pipeline.add(gvametaconvert)
    pipeline.add(gvametapublish)
    pipeline.add(converter2)

    logging.info("Linking elements in the Pipeline")
    
    videocrop.link(queue1)

    if USE_TIMEOVERLAY:
        timeoverlay = Gst.ElementFactory.make("timeoverlay", "timeoverlay")
        timeoverlay.set_property('time-mode', 'stream-time')
        pipeline.add(timeoverlay)
        queue1.link(timeoverlay)
        timeoverlay.link(converter1)
    else:
        queue1.link(converter1)

    converter1.link(capsfilter)
    capsfilter.link(face_detector)
    face_detector.link(queue2)
    queue2.link(gvaclassify1)
    gvaclassify1.link(queue3)
    queue3.link(gvaclassify2)
    gvaclassify2.link(queue4)
    queue4.link(gvametaconvert)
    gvametaconvert.link(gvametapublish)
    pad_metaconvert = gvametaconvert.get_static_pad("src")
    pad_metaconvert.add_probe(Gst.PadProbeType.BUFFER, pad_mc_probe_callback)

 # FAKESINK
    if sink == 'fake':
        # Make the fakesink
        fakesink = Gst.ElementFactory.make("fakesink", "fakesink")
        if not fakesink:
            logging.error("Unable to create fakesink")
        # Add and link fakesink
        pipeline.add(fakesink)
        gvametapublish.link(converter2)
        converter2.link(fakesink)
    else:
        # Make the fpscounter
        gvafpscounter = Gst.ElementFactory.make(
            "gvafpscounter", "gvafpscounter")
        if not gvafpscounter:
            logging.error("Unable to create gvafpscounter")

        # Add and link fpscounter
        pipeline.add(gvafpscounter)
        gvametapublish.link(gvafpscounter)

        # Make the gvawatermark
        gvawatermark = Gst.ElementFactory.make("gvawatermark", "gvawatermark")
        if not gvawatermark:
            logging.error("Unable to create gvawatermark")

        # Add and link watermark
        pipeline.add(gvawatermark)
        gvafpscounter.link(gvawatermark)
        gvawatermark.link(converter2)
        # Set callback on watermark buffer
        pad_watermark = gvawatermark.get_static_pad("src")
        pad_watermark.add_probe(Gst.PadProbeType.BUFFER, pad_wm_probe_callback)

        # Make the encoder
        encoder = Gst.ElementFactory.make("x264enc", "encoder")
        if not encoder:
            logging.error("Unable to create encoder")
        encoder.set_property("bitrate", 2048)
        encoder.set_property("speed-preset", 2)
        encoder.set_property("qp-min", 30)
        encoder.set_property("tune", "zerolatency")
        encoder.set_property("threads", 4)

        # Add and link encoder
        pipeline.add(encoder)
        converter2.link(encoder)

    # TCP server sink
    if sink == 'tcp':
        h264parse = Gst.ElementFactory.make("h264parse", "h264parse")
        if not h264parse:
            logging.error("Unable to create h264parse")
        pipeline.add(h264parse)
        encoder.link(h264parse)
        h264parse_src = h264parse.get_static_pad("src")

        mpegtsmux = Gst.ElementFactory.make("mpegtsmux", "mpegtsmux")
        if not mpegtsmux:
            logging.error("Unable to create mpegtsmux")
        # mpegtsmux.set_property("alignment", 7)
        pipeline.add(mpegtsmux)
        mpegtsmux_sink = mpegtsmux.get_request_pad("sink_1")
        h264parse_src.link(mpegtsmux_sink)

        tcpserversink = Gst.ElementFactory.make(
            "tcpserversink", "tcpserversink")
        if not tcpserversink:
            logging.error("Unable to create tcpserversink")
        tcpserversink.set_property("host", "0.0.0.0")
        tcpserversink.set_property("port", 9000)
        tcpserversink.set_property("sync", "false")
        pipeline.add(tcpserversink)
        mpegtsmux.link(tcpserversink)

    # RTSP server sink
    if sink == 'rtsp':
        # Make the rtppay
        rtppay = Gst.ElementFactory.make("rtph264pay", "rtppay")
        logging.info("Creating H264 rtppay")
        if not rtppay:
            logging.error("Unable to create rtppay")

        pipeline.add(rtppay)
        encoder.link(rtppay)

        # Make the UDP sink
        updsink = Gst.ElementFactory.make("udpsink", "udpsink")
        updsink.set_property('host', '224.224.255.255')
        updsink.set_property('port', updsink_port_num)
        updsink.set_property('async', False)
        updsink.set_property('sync', 1)

        pipeline.add(updsink)
        rtppay.link(updsink)
        # Start rtsp server
        init_rtsp_srv()

    source.connect("pad-added", on_new_rtsp_pad)
    pipeline.set_state(Gst.State.PAUSED)
    decoder.connect("pad-added", on_new_decoded_pad)
    pipeline.set_state(Gst.State.PAUSED)
    return pipeline


def init_rtsp_srv():
    # Start streaming
    rtsp_port_num = 9000

    server = GstRtspServer.RTSPServer.new()
    server.props.service = "%d" % rtsp_port_num
    server.attach(None)
    
    factory = GstRtspServer.RTSPMediaFactory.new()
    
    factory.set_launch(
        "( udpsrc name=pay0 port=%d buffer-size=524288 \
           caps=\"application/x-rtp, \
                  media=video, \
                  clock-rate=90000, \
                  encoding-name=(string)%s, payload=96 \" )" % (updsink_port_num, codec))
    
    factory.set_shared(True)
    server.get_mount_points().add_factory("/ds-test", factory)

    print("\n *** Launched RTSP Streaming at rtsp://localhost:%d/ds-test ***\n\n" %
          rtsp_port_num)


def glib_mainloop():
    mainloop = GLib.MainLoop()
    try:
        mainloop.run()
    except KeyboardInterrupt:
        pass


def bus_call(bus, message, pipeline):
    t = message.type
    if t == Gst.MessageType.EOS:
        logging.info("pipeline ended")
        pipeline.set_state(Gst.State.NULL)
        sys.exit()
    elif t == Gst.MessageType.ERROR:
        msg, dbg = message.parse_error()
        logging.error("error %s \n debug %s", msg, dbg)
        pipeline.set_state(Gst.State.PLAYING)
    elif t == Gst.MessageType.INFO:
        logging.info("msg %s", message)
    else:
        pass
    return True


def on_message(bus, message):
    if pipeline.get_clock():
        logging.info("on_message, type: %s", message.type)


def set_callbacks(pipeline):
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    # bus.connect("message::new-clock", on_message)
    bus.connect("message", bus_call, pipeline)


def on_new_decoded_pad(dbin, pad):
    string = pad.query_caps(None).to_string()
    print(string + '\n')
    decode = pad.get_parent()
    pipeline1 = decode.get_parent()
    videocrop = pipeline1.get_by_name('videocrop')
    decode.link(videocrop)
    logging.info("decodebin linked!")
    pipeline1.set_state(Gst.State.PLAYING)


def on_new_rtsp_pad(dbin, pad):
    string = pad.query_caps(None).to_string()
    print(string + '\n')
    source = pad.get_parent()
    pipeline = source.get_parent()
    if 'media=(string)video' in string:
        decoder = pipeline.get_by_name('decoder')
        source.link(decoder)
        logging.info("rtspsrc video linked!")
        
    elif 'media=(string)audio' in string:
        logging.info("rtspsrc audio linked!")

    pipeline.set_state(Gst.State.PLAYING)


if __name__ == '__main__':
    pipeline = init_pipe()
    set_callbacks(pipeline)
    pipeline.set_state(Gst.State.PLAYING)
    glib_mainloop()
    logging.info("Exiting")
