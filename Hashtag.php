<?php

namespace common\components\queryBuilder;

use yii\db\ActiveQuery;

interface Hashtag
{
    public function getTag(): string;

    public function __invoke(ActiveQuery $query): ActiveQuery;
}
