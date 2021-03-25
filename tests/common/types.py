# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


class SpaceDesc:
    def __init__(self,
                 name: str,
                 vid_type: str = "FIXED_STRING(32)",
                 partition_num: int = 7,
                 replica_factor: int = 1,
                 charset: str = "utf8",
                 collate: str = "utf8_bin"):
        self.name = name
        self.vid_type = vid_type
        self.partition_num = partition_num
        self.replica_factor = replica_factor
        self.charset = charset
        self.collate = collate

    def __str__(self):
        return str(self.__dict__)

    @staticmethod
    def from_json(obj: dict):
        return SpaceDesc(
            name=obj.get('name', None),
            vid_type=obj.get('vid_type', 'FIXED_STRING(32)'),
            partition_num=obj.get('partition_num', 7),
            replica_factor=obj.get('replica_factor', 1),
            charset=obj.get('charset', 'utf8'),
            collate=obj.get('collate', 'utf8_bin'),
        )

    def create_stmt(self) -> str:
        return f"""CREATE SPACE IF NOT EXISTS `{self.name}`( \
            partition_num={self.partition_num}, \
            replica_factor={self.replica_factor}, \
            vid_type={self.vid_type}, \
            charset={self.charset}, \
            collate={self.collate} \
        );"""

    def use_stmt(self) -> str:
        return f"USE `{self.name}`;"

    def drop_stmt(self) -> str:
        return f"DROP SPACE IF EXISTS `{self.name}`;"

    def is_int_vid(self) -> bool:
        return self.vid_type == 'int'


class Column:
    def __init__(self, index: int):
        if index < 0:
            raise ValueError(f"Invalid index of vid: {index}")
        self._index = index

    @property
    def index(self):
        return self._index


class VID(Column):
    def __init__(self, index: int, vtype: str, function: str = None):
        super().__init__(index)
        if vtype not in ['int', 'string']:
            raise ValueError(f'Invalid vid type: {vtype}')
        self._type = vtype
        if function not in [None, 'hash', 'uuid']:
            raise ValueError(f'Invalid vid function: {function}')
        self._function = function

    @property
    def id_type(self):
        return self._type

    @property
    def function(self):
        return self._function


class Rank(Column):
    def __init__(self, index: int):
        super().__init__(index)


class Prop(Column):
    def __init__(self, index: int, name: str, ptype: str):
        super().__init__(index)
        self._name = name
        if ptype not in ['string', 'int', 'double']:
            raise ValueError(f'Invalid prop type: {ptype}')
        self._type = ptype

    @property
    def name(self):
        return self._name

    @property
    def ptype(self):
        return self._type


class Properties:
    def __init__(self):
        self._name = ''
        self._props = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def props(self):
        return self._props

    @props.setter
    def props(self, props: list):
        if any(not isinstance(p, Prop) for p in props):
            raise ValueError("Invalid prop type in props")
        self._props = props


class Tag(Properties):
    def __init__(self):
        super().__init__()


class Edge(Properties):
    def __init__(self):
        super().__init__()
        self._src = None
        self._dst = None
        self._rank = None

    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, src: VID):
        self._src = src

    @property
    def dst(self):
        return self._dst

    @dst.setter
    def dst(self, dst: VID):
        self._dst = dst

    @property
    def rank(self):
        return self._rank

    @rank.setter
    def rank(self, rank: VID):
        self._rank = rank


class Vertex:
    def __init__(self):
        self._vid = None
        self.tags = []

    @property
    def vid(self):
        return self._vid

    @vid.setter
    def vid(self, vid: VID):
        self._vid = vid

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags: list):
        if any(not isinstance(t, Tag) for t in tags):
            raise ValueError('Invalid tag type of vertex')
        self._tags = tags
