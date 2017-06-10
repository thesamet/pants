# coding=utf-8
# Copyright 2017 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os
from copy import copy

from pex.interpreter import PythonInterpreter
from pex.pex import PEX
from pex.pex_builder import PEXBuilder

from pants.backend.python.python_requirement import PythonRequirement
from pants.backend.python.targets.python_requirement_library import PythonRequirementLibrary
from pants.backend.python.targets.python_target import PythonTarget
from pants.backend.python.tasks2.gather_sources import GatherSources
from pants.backend.python.tasks2.partition_targets import PartitionTargets
from pants.backend.python.tasks2.resolve_requirements import ResolveRequirements
from pants.backend.python.tasks2.resolve_requirements_task_base import ResolveRequirementsTaskBase
from pants.backend.python.tasks2.select_interpreter import SelectInterpreter
from pants.build_graph.address import Address
from pants.build_graph.resources import Resources
from pants.build_graph.target import Target
from pants.invalidation.cache_manager import VersionedTargetSet
from pants.util.dirutil import safe_concurrent_creation


class WrappedPEX(object):
  """Wrapper around a PEX that exposes only its run() method.

  Allows us to set the PEX_PATH in the environment when running.
  """

  _PEX_PATH_ENV_VAR_NAME = 'PEX_PATH'

  def __init__(self, pex, extra_pex_paths, interpreter):
    """
    :param pex: The main pex we wrap.
    :param extra_pex_paths: Other pexes, to "merge" in via the PEX_PATH mechanism.
    :param interpreter: The interpreter the main pex will run on.
    """
    self._pex = pex
    self._extra_pex_paths = extra_pex_paths
    self._interpreter = interpreter

  @property
  def interpreter(self):
    return self._interpreter

  def path(self):
    return self._pex.path()

  def cmdline(self, args=()):
    cmdline = ' '.join(self._pex.cmdline(args))
    pex_path = self._pex_path()
    if pex_path:
      return '{env_var_name}={pex_path} {cmdline}'.format(env_var_name=self._PEX_PATH_ENV_VAR_NAME,
                                                          pex_path=pex_path,
                                                          cmdline=cmdline)
    else:
      return cmdline

  def run(self, *args, **kwargs):
    kwargs_copy = copy(kwargs)
    env = copy(kwargs_copy.get('env')) if 'env' in kwargs_copy else {}
    env[self._PEX_PATH_ENV_VAR_NAME] = self._pex_path()
    kwargs_copy['env'] = env
    return self._pex.run(*args, **kwargs_copy)

  def _pex_path(self):
    return ':'.join(self._extra_pex_paths)


class PythonExecutionTaskBase(ResolveRequirementsTaskBase):
  """Base class for tasks that execute user Python code in a PEX environment.

  Note: Extends ResolveRequirementsTaskBase because it may need to resolve
  extra requirements in order to execute the code.
  """

  @classmethod
  def prepare(cls, options, round_manager):
    super(PythonExecutionTaskBase, cls).prepare(options, round_manager)
    round_manager.require_data(PartitionTargets.TARGETS_PARTITIONS)
    round_manager.require_data(SelectInterpreter.PYTHON_INTERPRETERS)
    round_manager.require_data(ResolveRequirements.REQUIREMENTS_PEX)
    round_manager.require_data(GatherSources.PYTHON_SOURCES)

  def extra_requirements(self):
    """Override to provide extra requirements needed for execution.

    Must return a list of pip-style requirement strings.
    """
    return []

  def create_pex(self, partition_name, targets, pex_info=None):
    """Returns a wrapped pex that "merges" the other pexes via PEX_PATH."""
    partition = self.target_roots_partitions()[partition_name]
    subset = partition.find_subset_for_targets(targets)
    relevant_targets = filter(
      lambda tgt: isinstance(tgt, (PythonRequirementLibrary, PythonTarget, Resources)),
      Target.closure_for_targets(subset))
    with self.invalidated(relevant_targets) as invalidation_check:

      # If there are no relevant targets, we still go through the motions of resolving
      # an empty set of requirements, to prevent downstream tasks from having to check
      # for this special case.
      if invalidation_check.all_vts:
        target_set_id = VersionedTargetSet.from_versioned_targets(
          invalidation_check.all_vts).cache_key.hash
      else:
        target_set_id = 'no_targets'

      interpreter = self.context.products.get_data(SelectInterpreter.PYTHON_INTERPRETERS)[
          partition_name][subset]
      path = os.path.join(self.workdir, str(interpreter.identity), target_set_id)
      extra_pex_paths_file_path = path + '.extra_pex_paths'
      extra_pex_paths = None

      # Note that we check for the existence of the directory, instead of for invalid_vts,
      # to cover the empty case.
      if not os.path.isdir(path):
        pexes = [
          self.context.products.get_data(ResolveRequirements.REQUIREMENTS_PEX)[
              partition_name][subset],
          self.context.products.get_data(GatherSources.PYTHON_SOURCES)[
              partition_name][subset]
        ]

        if self.extra_requirements():
          extra_reqs = [PythonRequirement(req_str) for req_str in self.extra_requirements()]
          import random
          addr = Address.parse('{}_extra_reqs'.format(self.__class__.__name__))
          self.context.build_graph.inject_synthetic_target(
            addr, PythonRequirementLibrary, requirements=extra_reqs)
          # Add the extra requirements first, so they take precedence over any colliding version
          # in the target set's dependency closure.
          pexes = [self.resolve_requirements(
              [self.context.build_graph.get_target(addr)], interpreter=interpreter)] + pexes

        extra_pex_paths = [pex.path() for pex in pexes if pex]

        with safe_concurrent_creation(path) as safe_path:
          builder = PEXBuilder(safe_path, interpreter, pex_info=pex_info)
          builder.freeze()

        with open(extra_pex_paths_file_path, 'w') as outfile:
          for epp in extra_pex_paths:
            outfile.write(epp)
            outfile.write(b'\n')

    if extra_pex_paths is None:
      with open(extra_pex_paths_file_path, 'r') as infile:
        extra_pex_paths = [p.strip() for p in infile.readlines()]
    return WrappedPEX(PEX(os.path.realpath(path), interpreter), extra_pex_paths, interpreter)
