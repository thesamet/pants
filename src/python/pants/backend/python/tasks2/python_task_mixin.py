from pants.backend.python.tasks2.partition_targets import PartitionTargets
from pants.backend.python.tasks2.select_interpreter import SelectInterpreter


class PythonTaskMixin(object):
  def group_id_for_targets(self, targets):
    partition = self.context.products.get_data(PartitionTargets.TARGETS_PARTITION)
    target_group_ids = [partition.ids_by_target[t] for t in targets]
    assert len(set(target_group_ids)) == 1, 'Targets must be in the same partition group.'
    return target_group_ids[0]

  def interpreter_for_targets(self, targets):
    interpreters = self.context.products.get_data(SelectInterpreter.PYTHON_INTERPRETERS)
    return interpreters[self.group_id_for_targets(targets)]
