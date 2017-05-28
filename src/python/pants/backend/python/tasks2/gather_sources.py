# coding=utf-8
# Copyright 2016 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os

from pex.interpreter import PythonInterpreter
from pex.pex import PEX
from pex.pex_builder import PEXBuilder

from pants.backend.python.tasks2.partition_targets import PartitionTargets
from pants.backend.python.tasks2.pex_build_util import dump_sources, has_python_sources
from pants.backend.python.tasks2.select_interpreter import SelectInterpreter
from pants.build_graph.target import Target
from pants.invalidation.cache_manager import VersionedTargetSet
from pants.task.task import Task
from pants.util.dirutil import safe_concurrent_creation


class GatherSources(Task):
  """Gather local Python sources for each subset in the partition.

  For each target subset, this task creates an (unzipped) PEX on disk containing the
  local Python sources of the closure of that subset.  This PEX can be merged with a requirements
  PEX to create a unified Python environment for running the relevant python code.

  Produces PYTHON_SOURCES, a mapping between subsets and the corresponding pex.
  """
  PYTHON_SOURCES = 'python_sources'

  @classmethod
  def implementation_version(cls):
    return super(GatherSources, cls).implementation_version() + [('GatherSources', 4)]

  @classmethod
  def product_types(cls):
    return [cls.PYTHON_SOURCES]

  @classmethod
  def prepare(cls, options, round_manager):
    round_manager.require_data(PartitionTargets.TARGETS_PARTITION)
    round_manager.require_data(SelectInterpreter.PYTHON_INTERPRETERS)
    round_manager.require_data('python')  # For codegen.

  def execute(self):
    if not self.context.products.is_required_data(self.PYTHON_SOURCES):
      return
    interpreters = self.context.products.get_data(SelectInterpreter.PYTHON_INTERPRETERS)
    partition = self.context.products.get_data(PartitionTargets.TARGETS_PARTITION)
    python_sources = self.context.products.get_data(self.PYTHON_SOURCES, lambda: {})
    for subset in partition.subsets:
      target_set = filter(has_python_sources, Target.closure_for_targets(subset))
      with self.invalidated(target_set) as invalidation_check:
         python_sources[subset] = self._get_pex_for_versioned_targets(
             interpreters[subset], invalidation_check.all_vts)

  def _get_pex_for_versioned_targets(self, interpreter, versioned_targets):
    if versioned_targets:
      target_set_id = VersionedTargetSet.from_versioned_targets(versioned_targets).cache_key.hash
    else:
      # If there are no relevant targets, we still go through the motions of gathering
      # an empty set of sources, to prevent downstream tasks from having to check
      # for this special case.
      target_set_id = 'no_targets'
    source_pex_path = os.path.realpath(os.path.join(self.workdir, target_set_id))
    # Note that we check for the existence of the directory, instead of for invalid_vts,
    # to cover the empty case.
    if not os.path.isdir(source_pex_path):
      # Note that we use the same interpreter for all targets: We know the interpreter
      # is compatible (since it's compatible with all targets in play).
      with safe_concurrent_creation(source_pex_path) as safe_path:
        self._build_pex(interpreter, safe_path, [vt.target for vt in versioned_targets])
    return PEX(source_pex_path, interpreter=interpreter)

  def _build_pex(self, interpreter, path, targets):
    builder = PEXBuilder(path=path, interpreter=interpreter, copy=True)
    for target in targets:
      dump_sources(builder, target, self.context.log)
    builder.freeze()
