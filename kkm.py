import faust 
import os
from datetime import datetime, timedelta
from typing import Optional, List
from datetimerange import DateTimeRange
import asyncio
from pytz import timezone
import binascii
from motor.motor_asyncio import AsyncIOMotorClient
from databases import DatabaseURL

MONGO_HOST = os.getenv("MONGO_HOST", "10.42.0.26")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASS = os.getenv("MONGO_PASSWORD", "admin")

MONGODB_URL = DatabaseURL(
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}"
)
db = AsyncIOMotorClient(str(MONGODB_URL))
localtz = timezone('Europe/Moscow')

KKM_TOPIC = 'KKM'
VISIT_TOPIC = 'face_visits_01'
TRACKS_TOPIC = 'test_tracks_00'
KAFKA_BROKER = 'kafka://10.42.0.26:9092'


class KKMVISITS(faust.App):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_kkm = {}
        self.table_visits = {}
        self.persist_table_visits = {}


app = KKMVISITS('faust-kkm-to-track-stage', version=2)


def ts_to_iso(utime: int):
    return datetime.fromtimestamp(utime).isoformat()


@app.on_configured.connect
def configure_from_settings(app, conf, **kwargs):
    conf.broker = KAFKA_BROKER
    conf.value_serializer = 'json'
    conf.store = 'memory://'
    conf.version = 2
    conf.topic_partitions = 1
    #    conf.autodiscover = True


class Visit(faust.Record, serializer='json'):
    track_id: str
    computer: str
    ap: str
    state: int
    face_id: str
    start_ts: Optional[datetime]
    last_ts: Optional[datetime]
    kkm_guid: str = None


class TrackLastTS(faust.Record, serializer='json'):
    track_id: str
    last_ts: int
    state: int = None


class KKMInKafka(faust.Record, serializer='json'):
    guid: str
    date: Optional[datetime]
    computer: str
    customer_id: str
    customer_phone: str
    start_ts_iso: Optional[datetime]
    time_range: DateTimeRange = None


persist_table_tracks = app.Table('Tracks0',value_type=Visit)


VISIT_TOPIC = app.topic(VISIT_TOPIC, value_type=Visit)
TRACKS_TOPIC = app.topic(
    TRACKS_TOPIC, key_serializer='raw', value_type=TrackLastTS)
KKM_TOPIC = app.topic(KKM_TOPIC, value_type=KKMInKafka)


@app.agent(KKM_TOPIC)
async def process_kkm(stream):
    async for kkm in stream:
        if kkm.computer == 'AP29Kassa1':
            kkm.time_range = DateTimeRange(kkm.start_ts_iso, kkm.date)
            kassa_tracks = app.table_visits.setdefault(
                kkm.computer.lower(), [])
            try:
                if visit := max([v for v in kassa_tracks if kkm.time_range.is_intersection(v.time_range) and v.kkm_guid is None],
                        key=lambda x: kkm.time_range.intersection(x.time_range).get_timedelta_second(), default=False):
                    print(kkm.time_range.intersection(visit.time_range).get_timedelta_second())
                    visit.kkm_guid = kkm.guid
                    app.persist_table_visits[visit.track_id] = visit
                    await db['vis']['vis'].replace_one({'_id': visit.db_id}, visit.asdict())
            except Exception as exc:
                print(exc)

@app.agent(VISIT_TOPIC)
async def process_visits(stream):
    async for track_id, vis in stream.items():
        vis.start_ts = localtz.localize(datetime.fromtimestamp(vis.start_ts))
        vis.last_ts = localtz.localize(datetime.fromtimestamp(vis.last_ts))
        
        vis.time_range = DateTimeRange(vis.start_ts, vis.last_ts)
        kassa_tracks = app.table_visits.setdefault(vis.computer.lower(), [])
        h = True
        for t in kassa_tracks:
            if t.track_id == vis.track_id:
                vis.last_ts = t.last_ts
                vis.time_range = t.time_range
                t = vis
                h = False
                db_vis = await db['vis']['vis'].insert_one(vis.asdict())
                vis.db_id = db_vis.inserted_id
        if h:
            db_vis = await db['vis']['vis'].insert_one(vis.asdict())
            vis.db_id = db_vis.inserted_id
            kassa_tracks.append(vis)

@app.agent(TRACKS_TOPIC)
async def process_tracks(stream):
    async for track_id, track in stream.items():
        local_last_ts = localtz.localize(datetime.fromtimestamp(track.last_ts))
        
        
        kassa_tracks = app.table_visits.setdefault(track_id.decode(
            encoding='UTF-8').lower().split('-')[0], [])
        for e in (t for t in kassa_tracks if t.track_id == track.track_id):
            e.last_ts = local_last_ts
            e.time_range = DateTimeRange(e.start_ts, e.last_ts)
            if track.state == 4:
              e.state = track.state
              await db['vis']['vis'].replace_one({'_id': e.db_id}, e.asdict())



@app.timer(interval=10.0)
async def every_10s():
    pass
    print(
        f'Events/s :{app.monitor.events_s} | avg event runtime {app.monitor.events_runtime_avg*1000:.2f}ms \n {len(app.persist_table_visits)} ')

if __name__ == '__main__':
    app.main()
