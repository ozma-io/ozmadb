using System;

namespace FunWithFlags.FunDBSchema.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class EntityAttribute : Attribute
    {
        public string MainField { get; }
        public bool ForbidExternalReferences { get; set; }
        public bool ForbidTriggers { get; set; }
        public bool IsHidden { get; set; }
        public bool IsFrozen { get; set; }
        public bool TriggersMigration { get; set; }

        public EntityAttribute(string mainField)
        {
            this.MainField = mainField;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple=true)]
    public class UniqueConstraintAttribute : Attribute
    {
        public string Name { get; }
        public string[] Columns { get; }

        public UniqueConstraintAttribute(string name, string[] columns)
        {
            this.Name = name;
            this.Columns = columns;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple=true)]
    public class CheckConstraintAttribute : Attribute
    {
        public string Name { get; }
        public string Expression { get; }

        public CheckConstraintAttribute(string name, string expression)
        {
            this.Name = name;
            this.Expression = expression;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple=true)]
    public class IndexAttribute : Attribute
    {
        public string Name { get; }
        public string[] Expressions { get; }
        public bool IsUnique { get; set; }

        public IndexAttribute(string name, string[] expressions)
        {
            this.Name = name;
            this.Expressions = expressions;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple=true)]
    public class ComputedFieldAttribute : Attribute
    {
        public string Name { get; }
        public string Expression { get; }
        public bool IsVirtual { get; set; }

        public ComputedFieldAttribute(string name, string expression)
        {
            this.Name = name;
            this.Expression = expression;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnFieldAttribute : Attribute
    {
        public string Type { get; }
        public bool IsImmutable { get; set; }
        public string? Default { get; set; }

        public ColumnFieldAttribute(string type)
        {
            this.Type = type;
        }
    }
}