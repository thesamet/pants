# coding=utf-8
# Copyright 2017 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

import os

from pants.backend.jvm.subsystems.jvm import JVM
from pants.backend.jvm.subsystems.shader import Shader
from pants.backend.jvm.tasks.jvm_tool_task_mixin import JvmToolTaskMixin
from pants.base.exceptions import TaskError

from pants.contrib.kythe.tasks.indexable_java_targets import IndexableJavaTargets


# Kythe requires various system properties to be set (sigh).  So we can't use nailgun.
class ExtractJava(JvmToolTaskMixin):
  _KYTHE_EXTRACTOR_MAIN = 'com.google.devtools.kythe.extractors.java.standalone.Javac8Wrapper'

  cache_target_dirs = True

  @classmethod
  def implementation_version(cls):
    # Bump this version to invalidate all past artifacts generated by this task.
    return super(ExtractJava, cls).implementation_version() + [('KytheExtract', 6), ]

  @classmethod
  def subsystem_dependencies(cls):
    return super(ExtractJava, cls).subsystem_dependencies() + (JVM.scoped(cls),)

  @classmethod
  def product_types(cls):
    # TODO: Support indexpack files?
    return ['kindex_files']

  @classmethod
  def prepare(cls, options, round_manager):
    super(ExtractJava, cls).prepare(options, round_manager)
    round_manager.require_data('runtime_classpath')
    round_manager.require_data('zinc_args')

  @classmethod
  def register_options(cls, register):
    super(ExtractJava, cls).register_options(register)
    cls.register_jvm_tool(register,
                          'kythe-extractor',
                          custom_rules=[
                            # These need to remain unshaded so that Kythe can interact with the
                            # javac embedded in its jar.
                            Shader.exclude_package('com.sun', recursive=True),
                          ],
                          main=cls._KYTHE_EXTRACTOR_MAIN)

  def execute(self):
    indexable_targets = IndexableJavaTargets.get(self.context)
    targets_to_zinc_args = self.context.products.get_data('zinc_args')

    with self.invalidated(indexable_targets, invalidate_dependents=True) as invalidation_check:
      cp = self.tool_classpath('kythe-extractor')
      for vt in invalidation_check.invalid_vts:
        self.context.log.info('Kythe extracting from {}\n'.format(vt.target.address.spec))
        javac_args = self._get_javac_args_from_zinc_args(targets_to_zinc_args[vt.target])
        jvm_options = list(JVM.scoped_instance(self).get_jvm_options())
        jvm_options.extend([
          '-DKYTHE_CORPUS={}'.format(vt.target.address.spec),
          '-DKYTHE_ROOT_DIRECTORY={}'.format(vt.target.target_base),
          '-DKYTHE_OUTPUT_DIRECTORY={}'.format(vt.results_dir)
        ])

        result = self.dist.execute_java(
          classpath=cp, main=self._KYTHE_EXTRACTOR_MAIN,
          jvm_options=jvm_options, args=javac_args, workunit_name='kythe-extract')
        if result != 0:
          raise TaskError('java {main} ... exited non-zero ({result})'.format(
            main=self._KYTHE_EXTRACTOR_MAIN, result=result))

    for vt in invalidation_check.all_vts:
      created_files = os.listdir(vt.results_dir)
      if len(created_files) != 1:
        raise TaskError('Expected a single .kindex file in {}. Got: {}.'.format(
          vt.results_dir, ', '.join(created_files) if created_files else 'none'))
      kindex_files = self.context.products.get_data('kindex_files', dict)
      kindex_files[vt.target] = os.path.join(vt.results_dir, created_files[0])

  @staticmethod
  def _get_javac_args_from_zinc_args(zinc_args):
    javac_args = []
    i = iter(zinc_args)
    arg = next(i, None)
    output_dir = None
    while arg is not None:
      arg = arg.strip()
      if arg in ['-d', '-cp', '-classpath']:
        # These are passed through from zinc to javac.
        javac_args.append(arg)
        javac_args.append(next(i))
        if arg == '-d':
          output_dir = javac_args[-1]
      elif arg.startswith('-C'):
        javac_args.append(arg[2:])
      elif arg.endswith('.java'):
        javac_args.append(arg)
      arg = next(i, None)
    # Strip output dir from classpaths.  If we don't then javac will read annotation definitions
    # from there instead of from the source files, which will cause the vnames to reflect the .class
    # file instead of the .java file.
    if output_dir:
      for i, a in enumerate(javac_args):
        if a in ['-cp', '-classpath']:
          javac_args[i + 1] = ':'.join([p for p in javac_args[i + 1].split(':') if p != output_dir])
    return javac_args
