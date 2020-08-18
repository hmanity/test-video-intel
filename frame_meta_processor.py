import faust
import asyncio
from models.Frame import Frame
from tracker_deepsort.detection import Detection
from tracker_deepsort.deepsort import DeepSort
from tracker_deepsort.tracker import Tracker
N = 1
app = faust.App(
    f'frame-meta-test-{N}',
    broker='kafka://10.42.0.26:9092',
    value_serializer='json',
    store='memory://',
    version=2, topic_partitions=1
)

FRAME_TOPIC = app.topic('AP29kassa1-archive-04-12')
TRACKS_TOPIC = app.topic('visits_test')


@app.agent(FRAME_TOPIC)
async def process_stream_frames(stream):
    ds_tracker = DeepSort()
    async for event in stream.events():
        async with event:
            detection_list = []
            frame = Frame(**event.value)
            for roi in frame.objects:
                det = Detection(
                    tlwh=[roi.x, roi.y, roi.w, roi.h],
                    confidence=roi.detection.confidence,
                    feature=await roi.get_face_feature(),
                    ts=frame.ts)

                detection_list.append(det)
            c, d = ds_tracker.update(detection_list)
            await TRACKS_TOPIC.send()

            # print(frame.dict())
            # if frame.objects is not None:
            #    await asyncio.sleep(0.1)
            #    det_data = frame.get_raw_det()
            #    a = ds_tracker.process(det_data)
            #    print(a)


@app.timer(interval=1.0)
async def every_1s():
    print(
        f'Events/s :{app.monitor.events_s} | avg event runtime {app.monitor.events_runtime_avg*1000:.2f}ms ')

if __name__ == "__main__":
    app.run()
