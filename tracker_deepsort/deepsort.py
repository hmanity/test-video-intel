from __future__ import absolute_import, division, print_function

import numpy as np
from .detection import Detection
from .nn_matching import NearestNeighborDistanceMetric
from .tracker import Tracker

#tt = np.asarray([np.concatenate([np.asarray([*[roi.__getattribute__(d) for d in ['x', 'y', 'w', 'h']], roi.detection.confidence,
#                                             frame.ts]), *[tensor.data for tensor in roi.tensors if tensor.name == 'face_id']]) for roi in frame.objects])


class DeepSort(object):
    '''
    Deep sort algorithm implementation taken from:
    https://github.com/nwojke/deep_sort

    - Arguments:
        - min_height: Only detections of height >= min_height will be \
            considered.
        - max_cosine_distance: Gating threshold for cosine distance of \
            features
        - nn_budget: Maximum size of the appeareance descriptors gallery. \
            If None, no budget is enforced.
    '''

    def __init__(self, min_height=0, max_cosine_distance=0.2,
                 nn_budget=None):
        self._min_height = min_height
        self._max_cosine_distance = max_cosine_distance
        self._nn_budget = nn_budget
        
        metric = NearestNeighborDistanceMetric(
            "cosine", self._max_cosine_distance, self._nn_budget
        )
        self._tracker = Tracker(metric)
        #super(DeepSort, self).__init__()

    def __call__(self):
        return self
    
    def update(self, detection_list):
        self._tracker.predict()
        self._tracker.update(detection_list)    

    def process(self, bboxes):
        '''
        - Arguments:
            - bboxes (np.array) (nb_boxes, 262). \
                The 262 is splitted as follows: [ top, left, width, height, ts, confidence, features...]

        - Returns:
            - tracks: (np.array) (nb_boxes, 5) \
                Specifically (nb_boxes, [top, left, width, height, track_id])
        '''
        detection_list = []
        dets_to_bboxes_d = {}
        dets_idx = -1
        for idx, bbox_data in enumerate(bboxes):
            bbox, ts, confidence, feature = bbox_data[0:
                                                      4], bbox_data[4], bbox_data[5], bbox_data[6:]
            if bbox[3] < self._min_height:
                continue
            detection_list.append(Detection(bbox, confidence, feature, ts, '', ''))
            dets_idx += 1
            dets_to_bboxes_d[dets_idx] = idx

        to_return = np.concatenate(
            [bboxes[:, 0:4], np.full((bboxes.shape[0], 1), -1)], axis=1)
        self._tracker.predict()
        matches, _, unmatched_dets = self._tracker.update(detection_list)
        tracks_to_dets_d = dict(matches)

        for idx, track in enumerate(self._tracker.tracks):
            if not track.is_confirmed() or track.time_since_update > 1:
                continue
            bbox = track.to_tlwh()
            new_box = np.array(
                [bbox[0], bbox[1], bbox[2], bbox[3], track.track_id], np.int32)
            box_idx = dets_to_bboxes_d.get(
                tracks_to_dets_d.get(idx, None), None)
            if box_idx is not None:
                to_return[box_idx] = new_box

        
        return np.array(to_return, np.int32)
