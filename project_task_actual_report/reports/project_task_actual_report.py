#  -*- coding: utf-8 -*-
#  Copyright 2020 Simone Rubino - Agile Business Group
#  License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).

from odoo import api, fields, models, tools


class ProjectTaskActualReport(models.AbstractModel):
    _name = 'project.task.actual.report'
    _description = "Actual time spent by tasks"
    _order = 'date'
    _rec_name = 'task_id'

    # _depends = {}
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

    @api.model_cr
    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""CREATE or REPLACE VIEW %s as (
select
    *
from
    (
    select
        message_id as id,
        task_id,
        message_id,
        date,
        case
            when new_name <> old_name
            -- Change counts as old value
 then old_name
            else first_value(new_name) over (partition by task_id,
            count_new_name)
        end "name",
        case
            when new_user <> old_user
            -- Change counts as old value
 then old_user
            else first_value(new_user) over (partition by task_id,
            count_new_user)
        end user_id,
        case
            when new_stage <> old_stage
            -- Change counts as old value
 then old_stage
            else first_value(new_stage) over (partition by task_id,
            count_new_stage)
        end stage_id,
        case
            when new_kanban_state <> old_kanban_state
            -- Change counts as old value
 then old_kanban_state
            else first_value(new_kanban_state) over (partition by task_id,
            count_new_kanban_state)
        end kanban_state,
        lag(date) over (partition by task_id
    order by
        date) as prev_update,
        extract(epoch
    from
        (date - lag(date) over (partition by task_id
    order by
        date)))/ 3600 as duration
    from
        (
        select
            *,
            count(new_name) over (partition by task_id
        order by
            message_id) count_new_name,
            count(new_user) over (partition by task_id
        order by
            message_id) count_new_user,
            count(new_stage) over (partition by task_id
        order by
            message_id) count_new_stage,
            count(new_kanban_state) over (partition by task_id
        order by
            message_id) count_new_kanban_state
        from
            (
            select
                mm.res_id task_id,
                mm.id message_id,
                mm."date" date,
                mtv_name.old_value_char old_name,
                mtv_name.new_value_char new_name,
                mtv_user.old_value_integer old_user,
                mtv_user.new_value_integer new_user,
                mtv_stage.old_value_integer old_stage,
                mtv_stage.new_value_integer new_stage,
                mtv_kanban_state.old_value_char old_kanban_state,
                mtv_kanban_state.new_value_char new_kanban_state
            from
                mail_message mm
            join mail_tracking_value mtv_name on
                mtv_name.mail_message_id = mm.id
                and mtv_name.field = 'name'
            join mail_tracking_value mtv_user on
                mtv_user.mail_message_id = mm.id
                and mtv_user.field = 'user_id'
            left join mail_tracking_value mtv_stage on
                mtv_stage.mail_message_id = mm.id
                and mtv_stage.field = 'stage_id'
            left join mail_tracking_value mtv_kanban_state on
                mtv_kanban_state.mail_message_id = mm.id
                and mtv_kanban_state.field = 'kanban_state'
            where mm.model = 'project.task'
        union
            select
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
            ) data
                ) filled_data 
                ) measured_data
where 
prev_update is not null
        )""" % (self._table,))
        return super(ProjectTaskActualReport, self).init()
