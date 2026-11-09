package gateway

// RoutesSnapshot — снимок routing map для диагностики и админ-эндпоинтов.
// Он показывает, куда Gateway отправил бы GET и PUT на данный момент: именно
// этим проверяется требование «gateway знает, куда маршрутизировать запросы».
type RoutesSnapshot struct {
	Revision       uint64            `json:"revision"`
	LocalRegion    string            `json:"local_region"`
	MasterRegion   string            `json:"master_region"`
	PartitionCount uint32            `json:"partition_count"`
	ReadRoutes     map[uint32]Leader `json:"read_routes"`
	WriteRoutes    map[uint32]Leader `json:"write_routes"`
}

// Ready сообщает, применима ли routing map. До первого удачного pull карты
// нет, и Gateway не должен обслуживать трафик.
func (s RoutesSnapshot) Ready() bool { return s.PartitionCount > 0 && s.Revision > 0 }

// MissingReads/MissingWrites считают партиции без маршрута. Ненулевые значения
// означают деградацию: часть партиций не имеет доступного лидера.
func (s RoutesSnapshot) MissingReads() int  { return int(s.PartitionCount) - len(s.ReadRoutes) }
func (s RoutesSnapshot) MissingWrites() int { return int(s.PartitionCount) - len(s.WriteRoutes) }

// Routes возвращает снимок текущей routing map. Данные не копируются: Leader —
// значение, а snapshot иммутабелен между Apply/ApplyView.
func (r *Router) Routes() RoutesSnapshot {
	cur := r.cur.Load()
	snapshot := RoutesSnapshot{LocalRegion: r.region}
	if cur == nil {
		return snapshot
	}
	snapshot.Revision = cur.revision
	snapshot.MasterRegion = cur.masterRegion
	snapshot.PartitionCount = cur.partitionCount
	snapshot.ReadRoutes = make(map[uint32]Leader, len(cur.local))
	for id, leader := range cur.local {
		snapshot.ReadRoutes[id] = leader
	}
	snapshot.WriteRoutes = make(map[uint32]Leader, len(cur.write))
	for id, leader := range cur.write {
		snapshot.WriteRoutes[id] = leader
	}
	return snapshot
}
