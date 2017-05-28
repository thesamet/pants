# coding=utf-8
# Copyright 2016 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os

from pex.interpreter import PythonInterpreter

from pants.backend.python.interpreter_cache import PythonInterpreterCache
from pants.backend.python.subsystems.python_setup import PythonSetup
from pants.backend.python.targets.python_library import PythonLibrary
from pants.backend.python.tasks2.gather_sources import GatherSources
from pants.backend.python.tasks2.partition_targets import PartitionTargets, TargetsPartition
from pants.backend.python.tasks2.select_interpreter import SelectInterpreter
from pants.build_graph.resources import Resources
from pants.python.python_repos import PythonRepos
from pants.util.dirutil import read_file
from pants_test.tasks.task_test_base import TaskTestBase


class GatherSourcesTest(TaskTestBase):
  def setUp(self):
      super(GatherSourcesTest, self).setUp()

  filemap = {
    'src/python/foo.py': 'foo_py_content',
    'src/python/bar.py': 'bar_py_content',
    'src/python/baz.py': 'baz_py_content',
    'resources/qux/quux.txt': 'quux_txt_content',
  }

  def make_targets(self):
    for rel_path, content in self.filemap.items():
      self.create_file(rel_path, content)

    sources1 = self.make_target(spec='//:sources1_tgt', target_type=PythonLibrary,
                                sources=['src/python/foo.py', 'src/python/bar.py'])
    sources2 = self.make_target(spec='//:sources2_tgt', target_type=PythonLibrary,
                                sources=['src/python/baz.py'])
    resources = self.make_target(spec='//:resources_tgt', target_type=Resources,
                                 sources=['resources/qux/quux.txt'])
    return [sources1, sources2, resources]

  @classmethod
  def task_type(cls):
    return GatherSources

  @staticmethod
  def pex_root(pex):
      return pex.cmdline()[1]

  def test_gather_sources_global(self):
    targets = self.make_targets()
    sources = self._gather_sources([targets])
    self.assertEquals([frozenset(targets)], sources.keys())
    pex_root = sources.items()[0][1]

    for target in targets:
      for source in target.sources_relative_to_source_root():
        content = read_file(os.path.join(pex_root, source))
        expected_content = self.filemap[source]
        self.assertEquals(expected_content, content)

  def test_gather_sources_singleton(self):
    targets = self.make_targets()
    sources = self._gather_sources([[t] for t in targets])
    for target in targets:
      pex_root = sources[frozenset([target])]
      for source in target.sources_relative_to_source_root():
        content = read_file(os.path.join(pex_root, source))
        expected_content = self.filemap[source]
        self.assertEquals(expected_content, content)

    self.assertFalse(
      os.path.exists(
        os.path.join(sources[frozenset([targets[0]])], 'src/python/baz.py')))

  def test_gather_sources_transitive(self):
    targets = self.make_targets()
    a = self.make_target(spec='//:a', target_type=Resources, dependencies=targets)
    pexes = self._gather_sources([[a]])

    for target in targets:
      for source in target.sources_relative_to_source_root():
        content = read_file(os.path.join(pexes[frozenset([a])], source))
        expected_content = self.filemap[source]
        self.assertEquals(expected_content, content)

  @staticmethod
  def _get_interpreter(context):
    # We must get an interpreter via the cache, instead of using PythonInterpreter.get() directly,
    # to ensure that the interpreter has setuptools and wheel support.
    interpreter = PythonInterpreter.get()
    interpreter_cache = PythonInterpreterCache(PythonSetup.global_instance(),
                                               PythonRepos.global_instance(),
                                               logger=context.log.debug)
    interpreters = interpreter_cache.setup(paths=[os.path.dirname(interpreter.binary)],
                                           filters=[str(interpreter.identity.requirement)])
    return interpreters[0]

  def _gather_sources(self, groups):
    context = self.context(target_roots=[tgt for group in groups for tgt in group],
                           for_subsystems=[PythonSetup, PythonRepos])
    interpreter = self._get_interpreter(context)
    partition = TargetsPartition(groups)
    context.products.get_data(PartitionTargets.TARGETS_PARTITION, lambda: partition)

    context.products.get_data(
        SelectInterpreter.PYTHON_INTERPRETERS,
        lambda: {subset: interpreter for subset in partition.subsets})

    context.products.require_data(GatherSources.PYTHON_SOURCES)

    task = self.create_task(context)
    task.execute()

    sources = context.products.get_data(GatherSources.PYTHON_SOURCES)

    return {subset: pex.cmdline()[1] for (subset, pex) in sources.items()}
