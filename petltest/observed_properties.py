# ===============================================================================
# Copyright 2020 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================

from petltest import post_item, get_item_by_name


def add_observed_property(name, op):

    # does this op already exist
    op_id = get_item_by_name('ObservedProperties', name)
    if op_id is None:
        # add the op
        op['name'] = name
        op_id = post_item('ObservedProperties', op)

    return op_id


# ============= EOF =============================================
