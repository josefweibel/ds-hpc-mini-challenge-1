# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: movies.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cmovies.proto\x12\x06movies\"8\n\x06Rating\x12\r\n\x05value\x18\x01 \x01(\x02\x12\x0f\n\x07movieId\x18\x02 \x01(\x05\x12\x0e\n\x06userId\x18\x03 \x01(\x05\",\n\nMeanRating\x12\r\n\x05value\x18\x01 \x01(\x02\x12\x0f\n\x07movieId\x18\x02 \x01(\x05\"2\n\x05Movie\x12\n\n\x02id\x18\x01 \x01(\x05\x12\r\n\x05title\x18\x02 \x01(\t\x12\x0e\n\x06genres\x18\x03 \x03(\tb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'movies_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _RATING._serialized_start=24
  _RATING._serialized_end=80
  _MEANRATING._serialized_start=82
  _MEANRATING._serialized_end=126
  _MOVIE._serialized_start=128
  _MOVIE._serialized_end=178
# @@protoc_insertion_point(module_scope)
