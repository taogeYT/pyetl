from context import EtlUtil
src_table = 'TEST'
dst_table = 'TE2'
field_map = {
    'ID': 'ID',
    'count1': 'count(ID)',
    'date': "to_char(TIME,'yyyy-mm-dd')"
}
job1 = EtlUtil(src_table, dst_table, field_map)

field_map = {
    'ID': 'ID',
    'count2': 'count(ID)',
    'date': "to_char(TIME,'yyyy-mm-dd')"
}
job2 = EtlUtil(src_table, dst_table, field_map)

field_map = {
    'ID': 'ID',
    'count3': 'count(ID)',
    'date': "to_char(TIME,'yyyy-mm-dd')"
}
job3 = EtlUtil(src_table, dst_table, field_map)

if __name__ == '__main__':
    '''
    groupby: 以id和时间为维度
    where: 统计的哪个类别
    '''
    rs1 = job1.run(where='SS=1', groupby="ID,to_char(TIME,'yyyy-mm-dd')")
    rs2 = job2.run(where='SS=2', groupby="ID,to_char(TIME,'yyyy-mm-dd')")
    rs3 = job3.run(where='SS=3', groupby="ID,to_char(TIME,'yyyy-mm-dd')")
    job1.join(rs1, rs2, rs3, on=['id', 'date'])
