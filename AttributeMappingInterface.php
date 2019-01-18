<?php

namespace common\components\queryBuilder;

interface AttributeMappingInterface
{
    public function attributeMap($attribute);

    public function getMappingByAttribute($attribute, $value);
}
