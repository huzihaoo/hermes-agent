"""CLI commands for template management."""

from hermes_cli.colors import Colors, color
from hermes_constants import get_hermes_home


def template_list(query: str = None, type_filter: str = None, hermes_home=None):
    """List templates with optional filtering."""
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    templates = store.list_recent(limit=100)
    
    # Apply filters
    if query:
        templates = [t for t in templates if query.lower() in t["name"].lower()]
    if type_filter:
        templates = [t for t in templates if t["task_type"] == type_filter]
    
    if not templates:
        filter_desc = []
        if query:
            filter_desc.append(f"名称包含 '{query}'")
        if type_filter:
            filter_desc.append(f"类型为 '{type_filter}'")
        filter_str = "、".join(filter_desc) if filter_desc else ""
        print(f"暂无{filter_str}的模板。")
        return
    
    print(color("📦 模板列表\n", Colors.BOLD))
    
    for tpl in templates:
        icon = {
            "coding": "💻",
            "docs": "📝",
            "research": "🔍",
            "chat": "💬",
            "cron": "⏰",
        }.get(tpl["task_type"], "❓")
        
        template_id = color(tpl["template_id"][:8], Colors.YELLOW)
        name = color(tpl["name"], Colors.BOLD)
        print(f"  {icon} {template_id} — {name}")
        
        params = tpl.get("params", {})
        if params:
            param_names = ", ".join(f"`{{{{{k}}}}}`" for k in params.keys())
            print(f"     参数: {param_names}")
    
    print()
    print(color("💡 使用 'hermes template show <id>' 查看模板详情", Colors.DIM))
    print(color("💡 使用 'hermes cron create <schedule> --template <id> --param key=value' 创建定时任务", Colors.DIM))


def template_show(template_id: str, hermes_home=None):
    """Show template details."""
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    
    # Try exact match first
    template = store.get(template_id)
    
    # If not found, try prefix match
    if not template:
        all_templates = store.list_recent(limit=100)
        matches = [t for t in all_templates if t["template_id"].startswith(template_id)]
        if len(matches) == 1:
            template = matches[0]
        elif len(matches) > 1:
            print(color(f"❌ 模板 ID '{template_id}' 匹配到多个模板，请使用更长的 ID。", Colors.RED))
            return
    
    if not template:
        print(color(f"❌ 模板 '{template_id}' 未找到。", Colors.RED))
        return
    
    # Display template details
    icon = {
        "coding": "💻",
        "docs": "📝",
        "research": "🔍",
        "chat": "💬",
        "cron": "⏰",
    }.get(template["task_type"], "❓")
    
    print(f"\n{icon} {color(template['name'], Colors.BOLD)}\n")
    print(f"  ID:         {color(template['template_id'], Colors.YELLOW)}")
    print(f"  类型:       {template['task_type']}")
    print(f"  来源任务:   {template['source_task_id']}")
    
    params = template.get("params", {})
    if params:
        print(f"\n  {color('参数:', Colors.BOLD)}")
        for param_name, param_def in params.items():
            required = color("必填", Colors.RED) if param_def.get("required", True) else color("可选", Colors.GREEN)
            default = param_def.get("default")
            if default is not None:
                print(f"    - `{{{{{param_name}}}}}` ({required}, 默认值: {color(default, Colors.CYAN)})")
            else:
                print(f"    - `{{{{{param_name}}}}}` ({required})")
    
    print(f"\n  {color('模板内容:', Colors.BOLD)}")
    print(f"    {template.get('request_summary', '')}")
    
    if params:
        print(f"\n  {color('使用示例:', Colors.BOLD)}")
        example_params = " ".join(f"--param {k}=<value>" for k in params.keys())
        print(f"    hermes cron create 'every 1d' --template {template['template_id'][:8]} {example_params}")
    
    print()


def template_delete(template_id: str, hermes_home=None):
    """Delete a template."""
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    
    # Try exact match first
    template = store.get(template_id)
    
    # If not found, try prefix match
    if not template:
        all_templates = store.list_recent(limit=100)
        matches = [t for t in all_templates if t["template_id"].startswith(template_id)]
        if len(matches) == 1:
            template = matches[0]
        elif len(matches) > 1:
            print(color(f"❌ 模板 ID '{template_id}' 匹配到多个模板，请使用更长的 ID。", Colors.RED))
            return
    
    if not template:
        print(color(f"❌ 模板 '{template_id}' 未找到。", Colors.RED))
        return
    
    # Delete the template
    template_id_full = template["template_id"]
    template_name = template["name"]
    
    if store.delete(template_id_full):
        print(color(f"✅ 模板 '{template_name}' ({template_id_full[:8]}) 已删除。", Colors.GREEN))
    else:
        print(color(f"❌ 删除模板失败。", Colors.RED))


def template_create(task_id: str, name: str, hermes_home=None):
    """Create a template from a successful task."""
    from gateway.tasks.template import TemplateStore
    from hermes_cli.task_trace import generate_receipt
    from gateway.tasks.types import TaskStatus
    import time
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    
    trace_file = hermes_home / "analytics" / "events.jsonl"
    receipt = generate_receipt(trace_file=trace_file, task_id=task_id)
    
    if receipt.started_at == 0:
        print(color(f"❌ 任务 '{task_id}' 未找到。", Colors.RED))
        return
    
    if receipt.status != TaskStatus.COMPLETED:
        print(color(f"❌ 只能从成功任务创建模板。任务 '{task_id}' 当前状态是 '{receipt.status.value}'。", Colors.RED))
        return
    
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    template_id = store.create_from_task(
        source_task_id=task_id,
        name=name,
        task_type=receipt.task_type.value,
        request_summary=receipt.request_summary,
        created_at=time.time(),
    )
    
    # Get the created template to show params
    template = store.get(template_id)
    params = template.get("params", {})
    
    print(color(f"✅ 模板已创建", Colors.GREEN))
    print(f"  模板 ID:  {color(template_id, Colors.YELLOW)}")
    print(f"  名称:     {name}")
    
    if params:
        print(f"\n  {color('提取的参数:', Colors.BOLD)}")
        for param_name in params.keys():
            print(f"    - `{{{{{param_name}}}}}`")
        
        print(f"\n  {color('使用示例:', Colors.BOLD)}")
        example_params = " ".join(f"--param {k}=<value>" for k in params.keys())
        print(f"    hermes cron create 'every 1d' --template {template_id[:8]} {example_params}")
    
    print()


def template_render(template_id: str, param_values: dict, hermes_home=None):
    """Render a template with parameter values."""
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    
    # Try exact match first
    template = store.get(template_id)
    
    # If not found, try prefix match
    if not template:
        all_templates = store.list_recent(limit=100)
        matches = [t for t in all_templates if t["template_id"].startswith(template_id)]
        if len(matches) == 1:
            template = matches[0]
        elif len(matches) > 1:
            print(color(f"❌ 模板 ID '{template_id}' 匹配到多个模板，请使用更长的 ID。", Colors.RED))
            return
    
    if not template:
        print(color(f"❌ 模板 '{template_id}' 未找到。", Colors.RED))
        return
    
    # Render template
    try:
        rendered = store.render(template["template_id"], param_values)
    except ValueError as e:
        print(color(f"❌ 渲染失败: {e}", Colors.RED))
        return
    
    print(color(f"✅ 模板渲染预览\n", Colors.GREEN))
    print(f"  模板: {color(template['name'], Colors.BOLD)} ({color(template['template_id'][:8], Colors.YELLOW)})")
    print(f"\n  {color('渲染结果:', Colors.BOLD)}")
    print(f"    {rendered}")
    print()


def template_edit(template_id: str, name: str = None, content: str = None, hermes_home=None):
    """Edit a template's name and/or content."""
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    
    # Try exact match first
    template = store.get(template_id)
    
    # If not found, try prefix match
    if not template:
        all_templates = store.list_recent(limit=100)
        matches = [t for t in all_templates if t["template_id"].startswith(template_id)]
        if len(matches) == 1:
            template = matches[0]
        elif len(matches) > 1:
            print(color(f"❌ 模板 ID '{template_id}' 匹配到多个模板，请使用更长的 ID。", Colors.RED))
            return
    
    if not template:
        print(color(f"❌ 模板 '{template_id}' 未找到。", Colors.RED))
        return
    
    if name is None and content is None:
        print(color("❌ 请至少提供 --name 或 --content 参数。", Colors.RED))
        return
    
    # Update template
    template_id_full = template["template_id"]
    if store.update(template_id_full, name=name, request_summary=content):
        print(color(f"✅ 模板 '{template['name']}' 已更新。\n", Colors.GREEN))
        if name:
            print(f"  新名称: {name}")
        if content:
            print(f"  新内容: {content}")
            # Show extracted params
            updated = store.get(template_id_full)
            params = updated.get("params", {})
            if params:
                print(f"  提取的参数: {', '.join(f'`{{{{{k}}}}}`' for k in params.keys())}")
        print()
    else:
        print(color(f"❌ 更新模板失败。", Colors.RED))


def template_export(template_id: str, hermes_home=None):
    """Export a template as JSON."""
    import json
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    
    # Try exact match first
    template = store.get(template_id)
    
    # If not found, try prefix match
    if not template:
        all_templates = store.list_recent(limit=100)
        matches = [t for t in all_templates if t["template_id"].startswith(template_id)]
        if len(matches) == 1:
            template = matches[0]
        elif len(matches) > 1:
            print(color(f"❌ 模板 ID '{template_id}' 匹配到多个模板，请使用更长的 ID。", Colors.RED))
            return
    
    if not template:
        print(color(f"❌ 模板 '{template_id}' 未找到。", Colors.RED))
        return
    
    # Export template
    template_id_full = template["template_id"]
    exported = store.export_template(template_id_full)
    if exported:
        print(json.dumps(exported, ensure_ascii=False, indent=2))
    else:
        print(color(f"❌ 导出模板失败。", Colors.RED))


def template_import(json_str: str, hermes_home=None):
    """Import a template from JSON."""
    import json
    from gateway.tasks.template import TemplateStore
    
    if hermes_home is None:
        hermes_home = get_hermes_home()
    store = TemplateStore(db_path=hermes_home / "analytics" / "templates.db")
    
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(color(f"❌ 无效的 JSON 格式: {e}", Colors.RED))
        return
    
    try:
        template_id = store.import_template(data)
        template = store.get(template_id)
        print(color(f"✅ 模板 '{template['name']}' 已导入。\n", Colors.GREEN))
        print(f"  模板 ID: {color(template_id[:8], Colors.YELLOW)}")
        print()
    except ValueError as e:
        print(color(f"❌ 导入失败: {e}", Colors.RED))
