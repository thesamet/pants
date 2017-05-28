# coding=utf-8
# Copyright 2016 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

from __future__ import (absolute_import, division, generators, nested_scopes, print_function,
                        unicode_literals, with_statement)

from collections import namedtuple

from pants.build_graph.target import Target
from pants.task.task import Task


class TargetsPartition(namedtuple('TargetsPartition', ['groups_by_id', 'ids_by_target'])):
  def group_ids(self):
    return self.groups_by_id.keys()

  def groups(self):
    return self.groups_by_id.values()

  @classmethod
  def create_from_groups(cls, groups):
    groups_by_id = {}
    ids_by_target = {}
    for group in groups:
      group_id = Target.maybe_readable_identify(group)
      groups_by_id[group_id] = frozenset(group)
      for target in group:
        ids_by_target[target] = group_id
    return TargetsPartition(groups_by_id, ids_by_target)

  def closure_for_group_id(self, group_id, **kwargs):
    return Target.closure_for_targets(self.groups_by_id[group_id], **kwargs)


class PartitionTargets(Task):
  TARGETS_PARTITION = 'targets_partition'

  STRATEGY_MINIMAL = 'minimal'
  STRATEGY_PER_TARGET = 'per_target'
  STRATEGY_GLOBAL = 'global'

  @classmethod
  def product_types(cls):
    return [cls.TARGETS_PARTITION]

  @classmethod
  def register_options(cls, register):
    super(PartitionTargets, cls).register_options(register)
    register('--strategy', choices=[
        cls.STRATEGY_MINIMAL,
        cls.STRATEGY_PER_TARGET,
        cls.STRATEGY_GLOBAL,
    ], default=cls.STRATEGY_GLOBAL)

  @classmethod
  def _minimal_partition(cls, targets):
    groups_by_head = {}

    closures = {target: target.closure() for target in targets}

    for target in targets:
      is_rep = all(target not in closures[other] or other == target
                   for other in targets)
      if is_rep:
        groups_by_head[target] = {target}

    for target in targets:
      for head in groups_by_head:
        if target in closures[head]:
          groups_by_head[head].add(target)
          break
      else:
        assert False, 'Target not in closure of any head.'

    return TargetsPartition.create_from_groups(groups_by_head.values())

  @classmethod
  def _per_target_partition(cls, targets):
    return TargetsPartition.create_from_groups([[t] for t in targets])

  @classmethod
  def _global_partition(cls, targets):
    return TargetsPartition.create_from_groups([targets])

  def execute(self):
    strategy = self.get_options().strategy
    if strategy == self.STRATEGY_MINIMAL:
      partition = self._minimal_partition(self.context.target_roots)
    elif strategy == self.STRATEGY_PER_TARGET:
      partition = self._per_target_partition(self.context.target_roots)
    elif strategy == self.STRATEGY_GLOBAL:
      partition = self._global_partition(self.context.target_roots)
    else:
      assert False, 'Unexpected partitioning strategy.'
    self.context.products.get_data(self.TARGETS_PARTITION, lambda: partition)
