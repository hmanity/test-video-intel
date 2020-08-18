from typing import Dict, List

import numpy as np
from pydantic import BaseModel, Field  # pylint: disable=E0611

# pylint: disable=E1136  # pylint/issues/3139


class _ArrayMeta(type):
    def __getitem__(self, t):
        return type('Array', (Array,), {'__dtype__': t})


class Array(np.ndarray, metaclass=_ArrayMeta):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate_type

    @classmethod
    def validate_type(cls, val):
        dtype = getattr(cls, '__dtype__', None)
        if isinstance(dtype, tuple):
            dtype, shape = dtype  # pylint: disable=E0633
        else:
            shape = tuple()

        result = np.array(val, dtype=dtype, copy=False, ndmin=len(shape))
        assert not shape or len(shape) == len(
            result.shape)  # ndmin guarantees this

        if any((shape[i] != -1 and shape[i] != result.shape[i]) for i in range(len(shape))):
            result = result.reshape(shape)
        return result


class Tensor(BaseModel):
    confidence: int = None
    label_id: int = None
    format: str = None
    layer_name: str = None
    label_id: int = None
    layout: str
    model_name: str = None
    name: str
    precision: str
    data: Array[np.float32] = None


class BoundingBox(BaseModel):
    x_max: float
    x_min: float
    y_max: float
    y_min: float


class Detection(BaseModel):
    bounding_box: BoundingBox
    confidence: float
    label_id: int
    label: str


class ROI(BaseModel):
    x: int
    y: int
    w: int
    h: int
    detection: Detection = None
    tensors: List[Tensor]
    id: int = None
    roi_type: str = None

    async def get_face_feature(self):
        return (tensor.data for tensor in self.tensors if tensor.name == 'face_id')


class Resolution(BaseModel):
    height: int
    width: int


class Tags(BaseModel):
    ap: str
    computer: str


class Frame(BaseModel):
    tags: Tags
    timestamp: str
    source: str
    resolution: Resolution
    ts: int
    objects: List[ROI] = Field(default_factory=list())

    def get_bboxes(self):
        return [np.asarray([*[roi.__getattribute__(d) for d in ['x', 'y', 'w', 'h']], self.ts, roi.detection.confidence]) for roi in self.objects]

    def get_features(self):
        return [[tensor.data for tensor in roi.tensors if tensor.name == 'face_id'][0] for roi in self.objects]

    def get_raw_det(self):
        bboxes = self.get_bboxes()
        features = self.get_features()
        return np.concatenate([bboxes, features], axis=1)
