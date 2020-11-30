#  -*- coding: utf-8 -*-
#  Copyright 2020 Simone Rubino - Agile Business Group
#  License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from odoo import api, fields, models, tools


TRACKED_TASK_FIELDS = [
    'name',
    'user_id',
    'stage_id',
    'kanban_state',
]
"""
These fields are defined as tracked (see track_visibility attribute) 
in the project.task model.
Editing this list automatically 
includes new data in the view underlying the below report.
In order to show them to the user, you should create new fields and views.
"""


class ProjectTaskActualReport(models.AbstractModel):
    _name = 'project.task.actual.report'
    _description = "Actual time spent by tasks"
    _order = 'date'
    _rec_name = 'task_id'

    _depends = {}
    task_id = fields.Many2one(
        comodel_name='project.task',
        readonly=True,
    )
    project_id = fields.Many2one(
        related='task_id.project_id',
        comodel_name='project.project',
        readonly=True,
    )
    message_id = fields.Many2one(
        comodel_name='mail.message',
        readonly=True,
    )
    date = fields.Datetime(
        readonly=True,
    )
    prev_update = fields.Datetime(
        readonly=True,
        string="Previous update",
    )
    name = fields.Char(
        readonly=True,
    )
    present_name = fields.Char(
        related='task_id.name',
        readonly=True,
    )
    user_id = fields.Many2one(
        comodel_name='res.users',
        readonly=True,
    )
    present_user_id = fields.Many2one(
        comodel_name='res.users',
        related='task_id.user_id',
        string="Present user",
        readonly=True,
    )
    stage_id = fields.Many2one(
        comodel_name='project.task.type',
        readonly=True,
    )
    present_stage_id = fields.Many2one(
        comodel_name='project.task.type',
        related='task_id.stage_id',
        string="Present stage",
        readonly=True,
    )
    kanban_state = fields.Char(
        readonly=True,
    )
    present_kanban_state = fields.Selection(
        related='task_id.kanban_state',
        string="Present kanban state",
        readonly=True,
    )
    duration = fields.Float()

    @api.model
    def _get_real_data_query(self):
        """Data gathered from DB."""
        task_model = self.env['project.task']

        tracked_fields = {ttf: task_model._fields.get(ttf)
                          for ttf in TRACKED_TASK_FIELDS}

        # See mail.tracking.value.create_tracking_values
        field_type_mapping = {
            'many2one': 'integer',
            'selection': 'char',
            'char': 'char',
        }
        select_clause = ["""select
    mm.res_id task_id,
    mm.id message_id,
    mm."date" date
"""]
        for field_name in TRACKED_TASK_FIELDS:
            tracked_field = tracked_fields.get(field_name)
            field_type = field_type_mapping.get(tracked_field.type)
            select_clause.append("""
    mtv_{field_name}.old_value_{field_type} old_{field_name},
    mtv_{field_name}.new_value_{field_type} new_{field_name}""".format(
                field_type=field_type,
                field_name=field_name,
            ))
        select_clause = ', '.join(select_clause)

        field_join_mapping = {
            'always': 'join',
            'onchange': 'left join',
        }
        from_clause = ["""from
    mail_message mm
"""]
        for field_name in TRACKED_TASK_FIELDS:
            tracked_field = tracked_fields.get(field_name)
            track_type = tracked_field.track_visibility
            join_type = field_join_mapping.get(track_type)
            from_clause.append("""
    {join_type} mail_tracking_value mtv_{field_name} on
        mtv_{field_name}.mail_message_id = mm.id
        and mtv_{field_name}.field = '{field_name}'""".format(
                field_name=field_name,
                join_type=join_type,
            ))
        from_clause = ' '.join(from_clause)

        where_clause = """where mm.model = 'project.task'"""
        return ' '.join([
            select_clause,
            from_clause,
            where_clause,
        ])

    @api.model
    def _get_present_data_query(self):
        """Create a record representing present state of every task."""
        return """select
    pt.id task_id,
    (
    select
        max(mail_message.id) as max_message_id
    from
        mail_message) + pt.id message_id,
    now() at time zone 'utc',
    pt.name,
    null,
    pt.user_id ,
    null,
    pt.stage_id ,
    null,
    pt.kanban_state,
    null
from
    project_task pt
join mail_message mm on
    pt.id = mm.res_id 
where mm.model = 'project.task'
"""

    @api.model
    def _get_data_query(self):
        """Query gathering all the data.

        This has a record representing every change occurred in every task,
        plus a record representing its present state."""
        return '{real_data} union {present_data}'.format(
            real_data=self._get_real_data_query(),
            present_data=self._get_present_data_query(),
        )

    @api.model
    def _get_partitioned_data_query(self):
        """Create a partition for each change.

        This will be used to fill NULL values in the rows."""
        # The underlying idea comes from
        # https://stackoverflow.com/a/19012333/11534960
        return """select
    *,
    count(new_name) 
    over (partition by task_id order by message_id) count_new_name,
    count(new_user_id)
    over (partition by task_id order by message_id) count_new_user_id,
    count(new_stage_id) 
    over (partition by task_id order by message_id) count_new_stage_id,
    count(new_kanban_state) 
    over (partition by task_id order by message_id) count_new_kanban_state
from ({data}) data
""".format(
            data=self._get_data_query(),
        )

    @api.model
    def _get_filled_data_query(self):
        """Fill empty values in rows with previous value."""
        # Second part ot the idea coming from
        # https://stackoverflow.com/a/19012333/11534960
        return """select
    message_id as id,
    task_id,
    message_id,
    date,
    case when new_name <> old_name -- Change counts as old value
    then old_name
    else first_value(new_name) 
        over (partition by task_id, count_new_name)
    end "name",
    case when new_user_id <> old_user_id
    then old_user_id
    else first_value(new_user_id) 
        over (partition by task_id, count_new_user_id)
    end user_id,
    case when new_stage_id <> old_stage_id
    then old_stage_id
    else first_value(new_stage_id) 
        over (partition by task_id, count_new_stage_id)
    end stage_id,
    case when new_kanban_state <> old_kanban_state
    then old_kanban_state
    else first_value(new_kanban_state) 
        over (partition by task_id, count_new_kanban_state)
    end kanban_state,
    lag(date) over (partition by task_id order by date) as prev_update,
    extract(epoch from 
        (date - lag(date) 
            over (partition by task_id order by date)))/ 3600 as duration
from ({partitioned_data}) partitioned_data
""".format(
            partitioned_data=self._get_partitioned_data_query(),
        )

    @api.model
    def _get_cleaned_data_query(self):
        """Clan data that are not interesting.

        For instance, the first record always has duration 0
        because there has been no previous event (prev_update is NULL)."""
        return """select 
    *
from ({filled_data}) filled_data
where 
    prev_update is not null
""".format(
            filled_data=self._get_filled_data_query(),
        )

    @api.model_cr
    def init(self):
        super(ProjectTaskActualReport, self).init()
        table_name = self._table
        tools.drop_view_if_exists(self.env.cr, table_name)
        self.env.cr.execute("""CREATE or REPLACE VIEW 
        {table_name} as ({cleaned_data})""".format(
            table_name=table_name,
            cleaned_data=self._get_cleaned_data_query(),
        ))
