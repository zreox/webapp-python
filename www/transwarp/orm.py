#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Database operation module. This module is independent with web module.
'''

import time, logging

import db

class Field(object):
    '''
    保存数据库中表的“字段属性”

    _count：类属性，每实例化一次，该值就 +1
    self._order：实例属性，实例化时从类属性 _count 处得到，用于记录该实例是该类的第几个实例
        例如在最后的 doctest 中：
            定义 user 时该类进行了 5 次实例化，来保存字段属性：
                id = IntegerField(primary_key=True)
                name = StringField()
                email = StringField(updatable=False)
                passwd = StringField(default=lambda: '******')
                last_modified = FloatField()
            最后各实例的 _order 属性就是这样的：
                INFO:root:[TEST _COUNT] name => 1
                INFO:root:[TEST _COUNT] passwd => 3
                INFO:root:[TEST _COUNT] id => 0
                INFO:root:[TEST _COUNT] last_modified => 4
                INFO:root:[TEST _COUNT] email => 2
            最后生成 sql 时（见 _gen_sql 函数），这些字段就是按序排列：
                create table `user` (
                `id` bigint not null,
                `name` varchar(255) not null,
                `email` varchar(255) not null,
                `passwd` varchar(255) not null,
                `last_modified` real not null,
                primary_key(`id`)
                );
    self.default：用于让 orm 自己填入缺省值，缺省值可以是可调用对象，比如函数
        例如：passwd 字段的默认值，就可以通过返回的函数调用取得
    其他的实例属性都是用来描述字段属性的
    '''
    _count = 0

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default =  kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count = Field._count + 1

    @property
    def default(self):
        '''
        利用 getter 实现的一个写保护的实例属性
        '''
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        '''
        返回实例对象的描述信息，比如：
            <IntegerField:id,bigint,default(0),UI>
            类：实例：实例 ddl 属性，实例 default 信息，3 种标志位：N U I
        '''
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)


class StringField(Field):
    '''
    保存 String 类型字段的属性
    '''
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)


class IntegerField(Field):
    '''
    保存 Integer 类型字段的属性
    '''
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0
        if 'ddl' not in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)


class FloatField(Field):
    '''
    保存 Float 类型字段的属性
    '''
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0.0
        if 'ddl' not in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)


class BooleanField(Field):
    '''
    保存 Boolean 类型字段的属性
    '''
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = False
        if 'ddl' not in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)


class TextField(Field):
    '''
    保存 Text 类型字段的属性
    '''
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['dll'] = 'text'
        super(TextField, self).__init__(**kw)


class BlobField(Field):
    '''
    保存 Blob 类型字段的属性
    '''
    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)


class VersionField(Field):
    '''
    保存 Version 类型字段的属性
    '''
    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')


_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])


def _gen_sql(table_name, mappings):
    '''
    类 ==> 表时，生成创建表的 sql
    '''
    pk = None
    sql = ['-- generating SQL for %s:' % table_name, 'create table `%s` (' % table_name]
    for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % f)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        # sql.append(nullable and '  `%s` %s,' % (f.name, ddl) or '  `%s` %s not null,' % (f.name, ddl))
        sql.append('  `%s` %s,' % (f.name, ddl) if nullable else '  `%s` %s not null,' % (f.name, ddl))
    sql.append('  primary key(`%s`)' % pk)
    sql.append(');')
    return '\n'.join(sql)


class ModelMetaclass(type):
    '''
    对类对象动态完成以下操作：
    避免修改 Model 类
        1. 排除对 Model 类的修改
    属性与字段的 mapping
        1. 从类的属性字典中提取出类属性和字段类的 mapping
        2. 提取完成后移除这些类属性，避免和实例属性冲突
        3. 新增 __mappings__ 属性，保存提取出来的 mapping 数据
    类和表的 mapping
        1. 提取类名，保存为表名，完成简单的类和表的映射
        2. 新增 __table__ 属性，保存提取出来的表名
    '''
    def __new__(cls, name, bases, attrs):
        # skip base Model class:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        # store all subclasses info:
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if name not in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping: %s => %s' % (k, v))
                # check duplicate primary key:
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable.')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable.')
                        v.nullable = False
                    primary_key = v
                mappings[k] = v
        # check exist of primary key:
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if '__table__' not in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if trigger not in attrs:
                attrs[trigger] = None
        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    '''
    这是一个基类，用户在子类中定义映射关系，因此我们需要动态扫描子类属性，
    从中抽取出类属性，完成 类 <==>表 的映射，这里使用 metaclass 来实现。
    最后将扫描出来的结果保存成类属性
        '__table__'：表名
        '__mappings__'：字段对象（字段的所有属性，见 Field 类）
        '__primary_key__'：主键字段
        '__sql__'：创建表时执行的 sql

    子类在实例化时，需要完成 实例属性 <==> 行值 的映射，这里使用定制 dict 来实现。
        Model 从字典继承而来，并且通过 __getattr__，__setattr__ 将 Model 重写，
        使其可以通过属性访问值，比如：a.key = value

    >>> class User(Model):
    ...     id = IntegerField(primary_key=True)
    ...     name = StringField()
    ...     email = StringField(updatable=False)
    ...     passwd = StringField(default=lambda: '******')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.last_modified = time.time()
    >>> u = User(id=300, name='Foo', email='orm@db.org')
    >>> r = u.insert()
    >>> u.email
    'orm@db.org'
    >>> u.passwd
    '******'
    >>> u.last_modified > (time.time()-2)
    True
    >>> f = User.get(300)
    >>> f.name
    u'Foo'
    >>> f.email
    u'orm@db.org'
    >>> f.email = 'changed@db.org'
    >>> r = f.update() # change email but email is non-updatable
    >>> len(User.find_all())
    1
    >>> g = User.get(300)
    >>> g.email
    u'orm@db.org'
    >>> r = g.delete()
    >>> len(db.select('select * from user where id=300'))
    0
    >>> import json
    >>> print User().__sql__()
    -- generating SQL for user:
    create table `user` (
      `id` bigint not null,
      `name` varchar(255) not null,
      `email` varchar(255) not null,
      `passwd` varchar(255) not null,
      `last_modified` real not null,
      primary key(`id`)
    );
    '''
    __metaclass__ = ModelMetaclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        '''
        get 时生效，比如 a[key]，a.get(key)；key 为 string
        get 时返回属性的值
        '''
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        '''
        set 时生效，比如 a[key] = value，a = {'key1': value1, 'key2': value2}
        set 时添加属性与值
        '''
        self[key] = value

    @classmethod
    def get(cls, pk):
        '''
        Get by primary key.
        '''
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, *args):
        '''
        通过 where 语句进行条件查询，返回 1 个 查询结果
        如有多个结果，仅取第一个；如没有结果，返回 None
        Like 'select * from table where arg=%s'
        '''
        d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    # def find_all(cls, *args):
    def find_all(cls):
        '''
        Find all and return list.
        '''
        L = db.select('select * from %s' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, *args):
        '''
        Find by where clause and return list.
        where 条件查询，返回 list
        '''
        L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        '''
        Find by 'select count(pk) from table' and return integer.
        返回数值
        '''
        return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        '''
        Find by 'select count(pk) from table where ...' and return int.
        '''
        return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

    def update(self):
        '''
        如果该行的字段属性有 updatable，代表该字段可以被更新
        用于定义的表（继承 Model 的类）是一个 Dict 对象，键值会变成实例的属性
        所以可以通过属性来判断用户是否定义了该字段的值
            如果有属性，则使用用户传入的值
            如果无属性，则调用字段对象的 default 属性传入
            具体见 Field 类的 default 属性

        通过 db 对象的 update 接口执行 SQL
            SQL: update `user` set `passwd`=%s,`last_modified`=%s,`name`=%s where id=%s,
                    ARGS: (u'******', 1508813773.294855, u'Foo', 300)
        '''
        self.pre_update and self.pre_update()
        L = []
        args = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.default
                    setattr(self, k, arg)
                L.append('`%s`=?' % k)
                args.append(arg)
        pk = self.__primary_key__.name
        args.append(getattr(self, pk))
        db.update('update `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
        return self

    def delete(self):
        '''
        通过 db 对象的 update 接口执行 SQL
            SQL: delete from `user` where `id`=%s, ARGS:(300,)
        '''
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk), )
        db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)
        return self

    def insert(self):
        '''
        通过 db 对象的 insert 接口执行 SQL
            SQL: insert into `user` (`passwd`,`last_modified`,`id`,`name`,`email`) values (%s,%s,%s,%s,%s),
                    ARGS: ('******', 1508813773.294855, 300, u'Foo', 'orm@db.org')
        '''
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        db.insert('%s' % self.__table__, **params)
        return self


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    db.create_engine('example', 'example', 'test')
    db.update('drop table if exists user')
    db.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    import doctest
    doctest.testmod()
