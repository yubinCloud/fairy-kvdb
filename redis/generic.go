package redis

import "errors"

func (rds *DataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *DataStructure) Type(key []byte) (RdsValueType, error) {
	encBuf, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encBuf) == 0 {
		return 0, errors.New("value is null")
	}
	return encBuf[0], nil
}
